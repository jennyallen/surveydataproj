#!/usr/bin/python3

import csv
import numpy as np
import time
import tldextract
from urllib.parse import urlparse
import sys
import os
import pandas as pd
import gc

def create_single_row(row):
    row = row.split('|')
    return row[:11] + [(('').join(row[11:])).strip()]

def get_best_guess_domain(url):
    extract_result = tldextract.extract(url)
    return extract_result.domain + '.' +  extract_result.suffix

def get_best_guess_subdomain(domain, url):
    if domain == "yahoo.com" and "news.yahoo.com" in url:
        return "news.yahoo.com"
    if domain == "msn.com" and "msn.com/en-us/news" in url:
        return "msn.com/en-us/news"
    subdomain = tldextract.extract(url).subdomain.replace('www.','')
    if len(subdomain) == 0:
        return None
    return '.'.join([subdomain, domain])

# need to get all possible domains because sometimes the domain extracted won't match what we have in the domain set
def get_possible_domain_options(url):
    url_parse = urlparse(url)
    if url_parse.path == '' or url_parse.path == "/":
        path_components = []
    elif url_parse.scheme == '':
        path_components = url_parse.path.split('/')[1:3]
    else:
        path_components = url_parse.path.split('/')[:3]
    extract_result = tldextract.extract(url)
    subdomain = extract_result.subdomain.replace('www.','')
    domain = extract_result.domain
    suffix = extract_result.suffix
    prefix_options = [domain + '.' + suffix]
    if len(subdomain) > 0:
        prefix_options.append('.'.join([subdomain, domain, suffix]))
    options = set()
    for prefix in prefix_options:
        options.add(prefix)
        option = prefix
        for component in path_components:
            if len(component) == 0:
                continue
            if component[0] != '/':
                component = '/' + component
            option = option + component
            options.add(option)
    return options

def get_url_domain_match(url, domain_set):
    domain_options = get_possible_domain_options(url)
    intersection =  domain_set.intersection(domain_options)
    if len(intersection) > 0:
        return intersection.pop()
    return None

def create_set_from_domain_file(file):
    with open(file, 'r', encoding="utf8") as f:
        reader = csv.reader(f)
        return set([line[0] for line in reader])

def get_news_category(fake_domain, msm_domain, msn_yahoo_homepage = None):
    if fake_domain != None and fake_domain != "":
        return "fake"
    elif msm_domain != None and msm_domain != "":
        return "msm"
    elif msn_yahoo_homepage:
        return "msn_yahoo_homepage"
    return "other"

def get_combined_category(media_label, category): 
    if media_label != "other":
        return media_label
    elif category == None or np.isnan(category):
        return "Unknown"
    return NOL_CATEGORY_DICT[int(category)]

def get_domain_subdomain_match(domain, subdomain, domain_set):
    if domain in domain_set:
        return domain
    if subdomain in domain_set:
        return subdomain
    return ""

def get_msn_yahoo_homepage(domain, url):
    if domain != "msn.com" and domain != "yahoo.com":
        return False
    extract_result = tldextract.extract(url)
    subdomain = extract_result.subdomain.replace('www','')
    if (subdomain != ''):
        return False
    url_parse = urlparse(url)
    return url_parse.path=="" or url_parse.path == "/"

def get_months(start, end = None):
    if end == None:
        return [start]
    elif (end - start) < 0:
        return "End must be before start!"
    start_y = start // 100
    end_y = end // 100
    years = end_y - start_y
    end_mo = end % 100
    start_mo = start % 100
    months = years * 12 + end_mo - start_mo
    dates = []
    d = start
    for x in range(0, months + 1):
        dates.append(d)
        d = d + 1
        if d % 100 > 12:
            d = (d // 100 + 1) * 100 + 1
    dates.reverse()
    return dates


