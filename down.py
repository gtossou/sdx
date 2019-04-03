import urllib.request

print('Beginning file download')

url = 'https://github.com/gtossou/sdx/blob/master/Brisbane_CityBike.json'  
urllib.request.urlretrieve(url, '/Brisbane_CityBike.json')  