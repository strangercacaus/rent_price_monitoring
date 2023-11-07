import scrapy
from bs4 import BeautifulSoup
from datetime import datetime

class VivarealSpider(scrapy.Spider):
    name = 'vivareal_sequential'
    allowed_domains = ['vivareal.com.br']
    start_urls = ['https://www.vivareal.com.br/aluguel/santa-catarina/florianopolis/?pagina=']
    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'FEEDS': {
            f'pocs/poc_scrapy/raw/vivareal_listings-{datetime.now()}.json': {
                'format': 'jsonlines',
                'overwrite': False,
                'append': True
            }
        }
    }

    def extract_listing_data(self,listing):
        title = listing.find('span', {'class': 'js-card-title'}).text.strip()
        price = listing.find('div', {'class': 'property-card__price'}).text.replace('R$','').replace('.','').split('/')[0]
        address = listing.find('span', {'class': 'property-card__address'}).text.replace('-',',').replace('|','').strip()

        return {
                'title': title,
                'price': price,
                'address': address,
            }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        soup = BeautifulSoup(response.text, features="html5lib")
        listings = soup.find_all('article', {'class': 'property-card__container js-property-card'})

        for listing in listings:
            yield self.extract_listing_data(listing)

        next_page = soup.find('button',{"class": 'js-change-page', "title": "Próxima página"})['data-page']
        if next_page is not None and int(next_page):
            next_page_url = f'{self.start_urls[0]}{(next_page)}'
            yield scrapy.Request(next_page_url, callback=self.parse)

# scrapy runspider pocs/poc_scrapy/poc_scrapy.py