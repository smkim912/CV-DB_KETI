# -*- coding: utf-8 -*-
# Author : Seongmin Kim , https://github.com/smkim912

from urllib2 import Request, urlopen
from urllib import urlencode, quote_plus, unquote

decode_key = unquote('YDkcRjBMq9adOc4KbTkmrPDQcc453843FdN%2FhCJGdZNL4jsCOP53gIkJC53ZMkxAAjM%2FDqmc1eiNi2ndRFL77Q%3D%3D')
url = 'http://data.ex.co.kr/exopenapi/trafficOprgPrcd/totalSpeed'
queryParams = '?' + urlencode({ 
				quote_plus('serviceKey') : decode_key,
				quote_plus('type') : 'json', 
				quote_plus('numOfRows') : '10', 
				quote_plus('pageNo') : '1',
				quote_plus('stDate') : '20151001', 
				quote_plus('edDate') : '20151001',
				quote_plus('iStartUnitCode') : '101',
				quote_plus('iEndUnitCode') : '115'
				})


request = Request(url + queryParams)
request.get_method = lambda: 'GET'
response_body = urlopen(request).read()
print response_body
