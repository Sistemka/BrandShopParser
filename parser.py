from gevent import monkey as curious_george
curious_george.patch_all(thread=False, select=False)
from functools import reduce
from threading import Thread
from pymongo import MongoClient
import requests
import grequests
import re
import random
from bs4 import BeautifulSoup
import asyncio
import aiohttp


proxies = set()

href_pool = set()
items_href_pool = set()
visited_href = set()
items_visited_href = set()

site = 'https://brandshop.ru/'

client = MongoClient('127.0.0.1', 27017)
db = client.parsed_data

PORT_REGEX = r'>([1-5]?[0-9]{2,4}|6[1-4][0-9]{3}|65[1-4][0-9]{2}|655[1-2][0-9]|6553[1-5])<'
IP_REGEX = r'>(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)<'
IP_PORT_REGEX = r'(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]):[0-9]'


async def fill_proxy_list(site):
    async with aiohttp.ClientSession() as session:
        async with session.get(site) as resp:
            page = await resp.text()
            proxies.update(re.findall(IP_PORT_REGEX, page))
            proxies.update(ip+':'+port for ip, port in zip(
                ['.'.join(ip) for ip in re.findall(IP_REGEX, page)], re.findall(PORT_REGEX, page)))


async def checkproxy(proxy: str):
    async with aiohttp.ClientSession() as session:
        try:
            resp = await session.get("https://google.com", proxy=proxy if "http://" in proxy else "http://"+proxy, timeout=0)
            resp.close()
        except:
            proxies.remove(proxy)


def fill_proxies(fill_proxy=False):
    if fill_proxy:
        with open("proxies.txt", "r") as f:
            tasks = []
            loop = asyncio.get_event_loop()
            for site in f:
                tasks.append(loop.create_task(fill_proxy_list(site)))
            loop.run_until_complete(asyncio.wait(tasks))
            loop.close()
    with open('file.txt', 'r') as f:
        proxies.update(f.read().split('\n'))
    proxies_list = {
        proxy if "http://" in proxy else "http://"+proxy for proxy in proxies}
    proxies.clear()
    proxies.update(proxies_list)
    responses = grequests.map([grequests.get(
        "http://167.172.189.250:8000/", proxies={'http': proxy}, timeout=2) for proxy in proxies])
    for response, proxy in zip(responses, list(proxies)):
        try:
            if response.status_code != 200:
                proxies.remove(proxy)
        except:
            proxies.remove(proxy)


def add_new_links(response):
    if response is not None:
        soup = BeautifulSoup(response.text, 'html.parser')
        for href in soup.findAll('a', href=True):
            if not ('http' in href['href'] and site not in href['href']):
                if 'goods' in href['href']:
                    items_href_pool.add(href['href'] if 'http' in href['href'] else site + href['href'])
                else:
                    href_pool.add(href['href'] if 'http' in href['href'] else site + href['href'])


def check_sex(text: str):
    text = text.lower()
    if 'муж' in text:
        return 'male'
    elif 'жен' in text:
        return 'female'
    elif 'дет' in text:
        return 'child'
    elif 'подрост' in text:
        return 'teenager'
    else:
        return 'undefined'


def clear_trash(product_info: dict):
    return {k: reduce(lambda t, r: t.replace(r, ' '), [v, '\xa0']).strip() if isinstance(v, str) else v for k, v in product_info.items()}


def parser(respone):  # for brandshop only
    product_info = {"images": [], 'name': None, 'price': None,
                    'sex': None, 'color': None, 'brand': None}
    page = respone.text
    soup = BeautifulSoup(page, 'html.parser')
    product_card = soup.findAll('div', {'class': 'product-card'})[0]
    images = product_card.findAll('img', alt=True)
    title = product_card.find('div', {'class': 'title'})
    # fill the map
    for image in images:
        try:
            product_info['images'].append(image.attrs["data-zoom-src"])
        except:
            product_info['images'].append(image.attrs["src"])
    product_info['name'] = product_card.find('span', {'itemprop': 'name'}).text
    product_info['price'] = int(reduce(lambda text, delete_val: text.replace(delete_val, ''), [
                                product_card.find('span', {'itemprop': 'price'}).text, 'р', ' ', '&nbsp;', '\xa0', '\n']))
    product_info['color'] = product_card.find(
        'div', {'itemprop': 'color'}).text
    product_info['brand'] = product_card.find(
        'span', {'itemprop': 'brand'}).text
    product_info['sex'] = check_sex(
        product_card.find('span', {'itemprop': 'name'}).text)
    return clear_trash(product_info)


def save_to_db(items: list):
    if len(items) > 0:
        db.items.insert_many(items)


def item_walker():
    items_cache = []
    while(True):
        requests_list = []
        current_href_pool = set(items_href_pool)
        for href in list(current_href_pool):
            if href not in items_visited_href:
                requests_list.append(grequests.get(
                    href, proxies={'http': random.sample(proxies, 1)[0]}))
            else:
                current_href_pool.remove(href)
        responses = grequests.map(requests_list)
        for response, link in zip(responses, current_href_pool):
            try:
                if response.status_code == 200:
                    items_visited_href.add(link)
                    items_cache.append(parser(response))
                    if len(items_cache) == 10:
                        save_to_db(items_cache)
                        items_cache = []
            except Exception as e:
                print(e)
                pass
        save_to_db(items_cache)
        items_cache = []


def walker():
    while(True):
        requests_list = []
        current_href_pool = set(href_pool)
        for link in list(current_href_pool):
            if link not in visited_href:
                requests_list.append(grequests.get(
                    link, proxies={'http': random.sample(proxies, 1)[0]}))
            else:
                current_href_pool.remove(link)
        responses = grequests.map(requests_list)
        for response, link in zip(responses, current_href_pool):
            try:
                if response.status_code == 200:
                    visited_href.add(link)
                    add_new_links(response)
            except Exception as e:
                print(e)
                pass


def run(site: str, get_proxies: bool = False):
    fill_proxies(get_proxies)
    add_new_links(requests.get(site))
    thread1 = Thread(target=walker)
    thread2 = Thread(target=item_walker)
    thread1.start()
    thread2.start()
    while(True):
        if href_pool == visited_href:
            thread1.join()
        if items_href_pool == items_visited_href:
            thread2.join()


run(site=site, get_proxies=True)