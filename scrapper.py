from facebook_page_scraper import Facebook_scraper

page_list = ['NacionalPauper']

proxy_port = 10001

posts_count = 250
browser = "firefox"

timeout = 600 #600 seconds
headless = False
filename = 'scrapper_nacional_pauper'

# Dir for output if we scrape directly to CSV
# Make sure to create this folder
directory = "/home/gomes/Documentos/Gomes/"

for page in page_list:
    #our proxy for this scrape
    # proxy = f'username:password@us.smartproxy.com:{proxy_port}'
    #initializing a scraper
    scraper = Facebook_scraper(page, posts_count, browser, timeout=timeout, headless=headless)
    
    #Running the scraper in two ways:
    
    # 1
    # Scraping and printing out the result into the console window:
    
    # json_data = scraper.scrap_to_json()
    # print(json_data)
    
    # 2
    # Scraping and writing into output CSV file:
    
    scraper.scrap_to_csv(filename, directory)
    
    # Rotating our proxy to the next port so we could get a new IP and avoid blocks
    proxy_port += 1
