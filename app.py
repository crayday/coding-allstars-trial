#!/usr/bin/env python3

import requests
from requests.adapters import Retry, HTTPAdapter
from bs4 import BeautifulSoup
import csv
from flask import Flask, request, redirect, send_file, abort
import re
import os
import sys
from celery import Celery, group
from collections import ChainMap
import redis
import json
from time import sleep, time

# Configuration
debug = True
host = 'https://www.coursera.org'

# -----------------------------------------------------------------------------
# Redis

r = None
def get_r():
    global r
    if not r:
        r = redis.Redis()
    return r

def save_course_data(category, url, course_data):
    string_data = json.dumps(course_data)
    get_r().hset(f'{category}:courses', url, string_data)

def has_courses(category):
    return bool(get_r().exists(f'{category}:courses'))

def get_courses(category):
    courses = get_r().hgetall(f'{category}:courses')
    for url, course_data in courses.items():
        courses[url] = json.loads(course_data)
    return courses

def set_processing_url(category, url):
    get_r().sadd(f'{category}:processing_urls', url)

def set_finished_url(category, url):
    get_r().sadd(f'{category}:finished_urls', url)
    get_r().srem(f'{category}:processing_urls', url)

def already_visited(category, url):
    return bool(get_r().sismember(f'{category}:processing_urls', url)
        or get_r().sismember(f'{category}:finished_urls', url))

def has_unfinished_urls(category):
    return bool(get_r().exists(f'{category}:processing_urls'))

def has_finished_urls(category):
    return bool(get_r().exists(f'{category}:finished_urls'))

# -----------------------------------------------------------------------------
# Celery app

celery = Celery('app', broker='pyamqp://')

@celery.task
def collect_and_save_to_csv(category):
    category = parse_category_name(category)
    url = f'/browse/{category}'
    set_processing_url(category, url)
    collect_from_category.delay(category, url)

    # wait while all urls are processed
    timeout = 30 * 60 # 30 min timeout
    start_time = time()
    while has_unfinished_urls(category):
        sleep(1)
        if time() - start_time > timeout:
            break

    courses = get_courses(category)
    log(f"Finished. Collected {len(courses)} courses")
    csv_path = path_to_csv(category)
    save_to_csv(courses, csv_path)

@celery.task
def collect_from_category(category, url):
    log(f"Collecting from the category by url {url}")
    soup = soup_from_url(url)
    for a in soup.find_all('a'):
        parse_link(category, a['href'])
    set_finished_url(category, url)

def parse_link(category, link_url):
    url_parts = link_url.split('/')
    if len(url_parts) < 3:
        return
    first_path_part = url_parts[1]
    if first_path_part == 'learn':
        set_processing_url(category, link_url)
        load_course.delay(category, link_url)
    elif first_path_part in {'specializations', 'professional-certificates'}:
        set_processing_url(category, link_url)
        load_and_collect_from_page.delay(category, link_url)

@celery.task
def load_and_collect_from_page(category, url):
    log(f"Loading & collecting from url {url}")
    soup = soup_from_url(url)

    # Load information about the course on the pag
    course_data = get_course_data(soup)

    if course_data:
        save_course_data(category, url, course_data)

    # Search for nested courses
    for a in soup.select('a[data-e2e=course-link]'):
        parse_link(category, a['href'])

    set_finished_url(category, url)

@celery.task
def load_course(category, url):
    log(f"Loading url {url}")
    soup = soup_from_url(url)
    course_data = get_course_data(soup)
    if course_data:
        save_course_data(category, url, course_data)
    set_finished_url(category, url)

def get_course_data(soup):
    try:
        category_nodes = soup.select('[role=navigation][aria-label=breadcrumbs] a')
        first_category = category_nodes[1]
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
        log_error(f"ERROR: Can't extract data about the course: {e}")
        return None

@celery.task
def save_to_csv(courses, filepath):
    log(f"Save to {filepath}.")
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
        for course in courses.values():
            writer.writerow([
                course['category'],
                course['name'],
                course['providers'],
                course['first_instructor'],
                course['description'],
                course['students_count'],
                course['ratings_count'],
            ])

# -----------------------------------------------------------------------------
# Helper functions & classes

def soup_from_url(url):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    session.mount('https://', HTTPAdapter(max_retries=retry))
    http_response = session.get(host+url)
    http_response.encoding = http_response.apparent_encoding
    html = http_response.text
    soup = BeautifulSoup(html, 'html.parser')
    return soup

def log(msg):
    if debug:
        print(msg)

def log_error(msg):
    sys.stderr.write(msg)

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

def path_to_csv(category):
    category = parse_category_name(category)
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, 'files', category+'.csv')

# -----------------------------------------------------------------------------
# Flask Web App

flask = Flask(__name__)

@flask.route('/category', methods=['POST'])
def post_category():
    category = request.form['category']
    if not category:
        abort(400)

    csv_path = path_to_csv(category)
    if not os.path.exists(csv_path):
        collect_and_save_to_csv.delay(category)

    return redirect(f"/category/{category}")

@flask.route('/category/<category>')
def category(category):
    category = parse_category_name(category)
    csv_path = path_to_csv(category)
    if os.path.exists(csv_path):
        return send_file(csv_path)
    elif has_finished_urls(category) \
            and not has_unfinished_urls(category) \
            and not has_courses(category):
        abort(404)
    else:
        return html_body("""
            Please wait, collecting data...
            <script>setTimeout(function(){window.location.reload(1);}, 5000);</script>
        """)



@flask.route('/')
def index():
    return html_body("""
      <form action="/category" method="POST">
        <div class="form-group">
          <label for="category">Category name</label>
          <input type="text" class="form-control" id="category" name="category" placeholder="Data Science"/>
        </div>
        <button type="submit" class="btn btn-primary">Collect</button>
      </form>
    """)

def html_body(html):
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
      """ + html + """
    </div></div>
  </body>
</html>
"""

if __name__ == '__main__':
    flask.run(host='0.0.0.0')
