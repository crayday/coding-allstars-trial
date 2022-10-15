#!/usr/bin/env python3

import requests
from requests.adapters import Retry, HTTPAdapter
from bs4 import BeautifulSoup
import csv
from flask import Flask, request, redirect, send_file, abort
import re
import os
import sys

class CourseCollecter:
    def __init__(self, host, debug=False, limit=None):
        self.host = host
        self.courses = {}
        self.browsed_pages = set()
        self.debug = debug
        self.limit = limit
        self.current_category_url = None
        self.check_category = False
        self.session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        self.session.mount('https://', HTTPAdapter(max_retries=retry))

    def collect_from_category(self, url):
        self.log(f"Collecting from the category by url {url}")
        self.current_category_url = url
        soup = self.load_soup(url)
        for a in soup.find_all('a'):
            if self.limit and len(self.courses) >= self.limit:
                break
            self.parse_link(a['href'])

    def parse_link(self, link_url):
        url_parts = link_url.split('/')
        if link_url in self.browsed_pages or len(url_parts) < 2:
            return
        first_path_part = url_parts[1]
        #if first_path_part in {'learn', 'projects'}:
        if first_path_part == 'learn':
            course_data = self.load_course(link_url)
            if course_data:
                self.courses[link_url] = course_data
        elif first_path_part in {'specializations', 'professional-certificates'}:
            self.load_and_collect_from_page(link_url)
        self.browsed_pages.add(link_url)

    def load_and_collect_from_page(self, url):
        self.log(f"Loading & collecting from url {url}")
        soup = self.load_soup(url)

        # First load information about the course on the pag
        course_data = self.get_course_data(soup)
        if course_data:
            self.courses[url] = course_data

        # Then search for nested courses
        for a in soup.select('a[data-e2e=course-link]'):
            if self.limit and len(self.courses) >= self.limit:
                break
            self.parse_link(a['href'])

    def load_course(self, url):
        self.log(f"Loading url {url}")
        soup = self.load_soup(url)
        return self.get_course_data(soup)

    def get_course_data(self, soup):
        try:
            category_nodes = soup.select('[role=navigation][aria-label=breadcrumbs] a')
            first_category = category_nodes[1]
            # Check if the course really belongs to the requested category
            if self.check_category and first_category['href'] != self.current_category_url:
                self.log(f"Course {url} doesn't belong to requested category {self.current_category_url}")
                return None
            last_category = category_nodes[-1]
            category = last_category.get_text().strip()

            name = get_text_by_css(soup, '[data-test=banner-title-container]')
            ratings_count = get_text_by_css(soup, '[data-test=ratings-count-without-asterisks]')
            ratings_count = text_to_int(ratings_count)
            students_count = get_text_by_css(soup, '.rc-ProductMetrics')
            students_count = text_to_int(students_count)

            first_instructor_node = soup.select('.rc-InstructorListSection .instructor-name')[0]
            first_instructor_texts = first_instructor_node.find_all(text=True, recursive=False)
            first_instructor = ' '.join((t.get_text().strip() for t in first_instructor_texts))

            description = get_text_by_css(soup, '.description')
            providers = get_texts_by_css(soup, '.PartnerList h3')
            providers_str = ', '.join(providers)
            return {
                'name': name,
                'category': category,
                'first_instructor': first_instructor,
                'providers': providers_str,
                'ratings_count': ratings_count,
                'students_count': students_count,
                'description': description,
            }
        except IndexError as e:
            self.log_error(f"ERROR: Can't extract data about the course: {e}")
            return None

    def save_to_csv(self, filepath):
        with open(filepath, mode='w') as fh:
            writer = csv.writer(fh, delimiter=',', quotechar='"')
            writer.writerow([
                'Category Name',
                'Course Name',
                'Course Provider',
                'First Instructor Name',
                'Course Description',
                '# of Students Enrolled',
                '# of Ratings',
            ])
            for course in self.courses.values():
                writer.writerow([
                    course['category'],
                    course['name'],
                    course['providers'],
                    course['first_instructor'],
                    course['description'],
                    course['students_count'],
                    course['ratings_count'],
                ])

    def load_soup(self, url):
        http_response = self.session.get(self.host+url)
        http_response.encoding = http_response.apparent_encoding
        html = http_response.text
        soup = BeautifulSoup(html, 'html.parser')
        return soup

    def log(self, msg):
        if self.debug:
            print(msg)

    def log_error(self, msg):
        sys.stderr.print(msg)

# -----------------------------------------------------------------------------
# Helper functions

def get_text_by_css(soup, css_selector):
    return soup.select(css_selector)[0].get_text().strip()

def get_texts_by_css(soup, css_selector):
    return (node.get_text().strip() for node in soup.select(css_selector))

def text_to_int(string):
    return int(re.sub(r'\D+', '', string))

def parse_category_name(category):
    # Replace all non alpha symbols with dashes
    category = category.strip().lower()
    return re.sub(r'[^a-z0-9-]+', '-', category)

def collect_from_category(category):
    category = parse_category_name(category)
    debug = False
    limit = None
    course_collector = CourseCollecter('https://www.coursera.org', debug=debug, limit=limit)
    course_collector.collect_from_category(f'/browse/{category}')
    courses_count = len(course_collector.courses)
    if debug:
        print(f"Finished. Collected {courses_count} courses")
    if courses_count > 0:
        csv_path = path_to_csv(category)
        course_collector.save_to_csv(csv_path)
        if debug:
            print(f"Save to {csv_path}.")
    return courses_count

def path_to_csv(category):
    category = parse_category_name(category)
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, 'files', category+'.csv')

# -----------------------------------------------------------------------------
# Flask Web App

app = Flask(__name__)

@app.route('/category', methods=['POST'])
def post_category():
    category = request.form['category']
    if not category:
        abort(400)

    csv_path = path_to_csv(category)
    if not os.path.exists(csv_path):
        courses_count = collect_from_category(category)
        if not courses_count:
            abort(404)

    return redirect(f"/category/{category}")

@app.route('/category/<category>')
def category(category):
    csv_path = path_to_csv(category)
    if os.path.exists(csv_path):
        return send_file(csv_path)
    else:
        abort(404)

@app.route('/')
def index():
    return """
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Coding AllStars Python Trial Task by Egor Rodygin</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">
  </head>
  <body>
    <div class="container"><div class="jumbotron">
      <h1>Coding AllStars Python Trial Task by Egor Rodygin</h1>
      <form action="/category" method="POST">
        <div class="form-group">
          <label for="category">Category name</label>
          <input type="text" class="form-control" id="category" name="category" placeholder="Data Science"/>
        </div>
        <button type="submit" class="btn btn-primary">Collect</button>
      </form>
    </div></div>
  </body>
</html>
"""

if __name__ == '__main__':
    app.run()
