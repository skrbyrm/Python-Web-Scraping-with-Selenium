# Python-Web-Scraping-with-Selenium
This Python code scrapes data from a website that requires login authentication using Selenium, logs into the website, scrapes data from a table, converts it to a Pandas dataframe, and stores the data in an Excel file. The code consists of several functions:

- excel_to_json() - a function that reads an Excel file and converts it to JSON
- df_to_excel() - a function that writes a Pandas dataframe to an Excel file
- init_browser() - a function that initializes a Firefox browser using the GeckoDriverManager package
- login() - a function that logs into the website using the browser object and user credentials
- click_element() - a function that clicks an element on a web page using the browser object and element XPath
- scrape_data() - a function that scrapes data from a website table, converts it to a Pandas dataframe, and returns the data
- The code;
 | uses the Selenium library to automate web browsing and data scraping.
 | logs into a website using a user's credentials, navigates to a specific page, and scrapes data from a table.
 | stores the scraped data in an Excel file and MySQL database.
 | uses the pandas library to manipulate data and the sqlalchemy library to interact with a MySQL database.
 | the webdriver_manager package to install and manage the GeckoDriver executable.
