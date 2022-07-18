from dagster import (op, Field, String, Int)
from real_estate.common.real_estate_type import real_estate_dataframe
from bs4 import BeautifulSoup
import requests
import json
import urllib.request as urllib2
import random
import re
import time
from pyspark.sql import Row

@op(
    description = '''This will scrape the required data from the WG website''',
    config_schema = {
        'scrape_url': Field(
            String,
            default_value = "https://www.wg-gesucht.de/wg-zimmer-in-Bremen.17.0.1.1.html",
            is_required = False,
            description = ('''The URL in which we have to scrape the WG information'''),
        ),
        'pages': Field(
            Int,
            default_value = 2,
            description = ('''This is the number of pages you want to scrape'''),
        ),
    },
)
def scrap_wg(context) -> real_estate_dataframe:
    context.log.info("Running the scrap_wg() OP")
    headers= {
        'user-agent' : 'Mozilla/5.0'
    }
    all_ids = []
    prices = {}
    names = {}
    sizes = {}
    info = {}
    property_list = []
    for i in range(1, context.op_config['pages']):
        url = context.op_config['scrape_url'] + f"#page-{i}"
        raw_data= requests.post(url, headers = headers)
        soup = BeautifulSoup(raw_data.text, "html.parser")
        span_tags = soup.findAll("span")
        ids = []
        for tag in span_tags:
            string = str(tag)
            if string.find('back_to_ad') >= 0:
                ids.append(re.findall("\d+", str(tag))[0])
                all_ids.append(re.findall("\d+", str(tag))[0])
    for i in all_ids:
        post = soup.find("div", {"data-id": f"{i}"})
        name = post.find("h3", {"class" : "truncate_title noprint"})
        names[i] = (str(name.get_text().strip()))
        price = post.find("div", {"class" : "col-xs-3"})
        price = (str(price.get_text().strip()))
        prices[i] = price
        size = post.find("div", {"class" : "col-xs-3 text-right"})
        size = str(size.get_text().strip())
        sizes[i] = size
        information = post.find("div", {"class" : "col-xs-11"})
        information = information.get_text().split('|')
        information = [info.replace('\n', '').strip() for info in information]
        information[1] = ' '.join(information[1].split())
        info[i] = information
    for i in all_ids:
        property_list.append({
            'property_id' : int(i),
            'name': names[i],
            'price': prices[i],
            'size': sizes[i],
            'extra_information': str(info[i])
        })
    return property_list

