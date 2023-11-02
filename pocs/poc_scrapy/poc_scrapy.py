import scrapy
from bs4 import BeautifulSoup

class VivarealSpider(scrapy.Spider):
    name = 'vivareal_sequential'
    allowed_domains = ['vivareal.com.br']
    start_urls = ['https://www.vivareal.com.br/aluguel/santa-catarina/florianopolis/?pagina=']
    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'FEEDS': {
            'vivareal_listings.json': {
                'format': 'json',
                'overwrite': False,
                'append': True
            }
        }
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        starting_page = 1
        soup = BeautifulSoup(response.text, features="html5lib")
        listings = soup.find_all('article', {'class': 'property-card__container js-property-card'})

        for listing in listings:
            title = listing.find('span', {'class': 'js-card-title'}).text.strip()
            price = listing.find('div', {'class': 'property-card__price'}).text.replace('R$','').replace('.','').split('/')[0]
            address = listing.find('span', {'class': 'property-card__address'}).text.replace('-',',').replace('|','').strip()

            yield {
                'title': title,
                'price': price,
                'address': address,
            }

        next_page = soup.find('button', class_='js-change-page', title='Próxima página')['data-page']
        if next_page is not None:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(next_page_url, callback=self.parse)

# scrapy runspider pocs/poc_scrapy.py