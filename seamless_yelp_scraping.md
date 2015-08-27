These functions are part of larger collection of functions that 
have not yet been made publicly available. Thus, it is unlikely 
these scripts will work without commenting some of the missing
modules, integrating those adjustments, and recreating some of 
the environmental variables, e.g., database authentication parameters.

Some other dependencies include:

* a number of [pip libraries](http://pastebin.com/7maudUMn)
* PhantomJS 1.9.8
* ChromeDriver 2.15

### Generally
The functions called by the arguments below initially query a 
database to obtain a list of addresses corresponding to a lattice 
of points generated over the target area.

#### Scraping www.seamless.com

1. ###### Use address to search area; collect vendor page links and some preliminary data

	* `python scrape_vendors.py sl 1`
	* `python scrape_vendors.py sl search_results`

	primary objective of this function is to get urls 
    for each vendor page

2. ###### Search Google with name of vendor using site:www.seamless.com to obtain link

    * `python scrape_vendors.py sl 2`
    * `python scrape_vendors.py sl prev_closed_v`

	this is necessary when a vendor is closed when pulling links 
    from search results, where vendor link is not available 

3. ###### Use vendor urls to pull all data for each vendor, parse and save

    *  `python scrape_vendors.py sl 3`
    *  `python scrape_vendors.py sl v_pgs`

    except for food menu items and cost, just about all data for 
    each vendor is collected and sanitized


#### Scraping www.yelp.com

0. ###### Use address to search area; collect vendor page links and some preliminary data

	* `python scrape_vendors.py y 0`
	* `python scrape_vendors.py y search_results`

	note that this is function zero for yelp scraping; this 
    method is primarily a backup solution should the next 
    function fail

1. ###### Use address in Yelp API; collect data

	* `python scrape_vendors.py y 1`
	* `python scrape_vendors.py y api`

	this function is much faster than above but there is a query limit

2. ###### Use address in Yelp API; collect data

	* `python scrape_vendors.py y 2`
	* `python scrape_vendors.py y v_pgs`

	this function uniquely collects hours and all comments per vendor



