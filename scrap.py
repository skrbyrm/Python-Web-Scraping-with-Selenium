# Importing required libraries
import pathlib
import time
import json
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import selenium
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Defining some variables
url = 
url2 = 
elm_xpath_1 = 
elm_xpath_2 = 
elm_xpath_3 = 
table_xpath = 
firm_list = 

dfbase=pd.DataFrame()
dfall=pd.DataFrame()
error_dict = []

org_name = 'CLK_'
sheet = datetime.now().strftime("%H_%M")
file_name = datetime.now().strftime("%m_%d_%Y_%H")
data_path = 'Data/' + org_name + file_name +'.xlsx'

db_user = ''
db_password = ''
db_host = ''
db_port = ''
db_name = ''
table_name = ''

engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}", echo=False)
df_sql = pd.read_sql_table(table_name, con=engine)

# Function to read excel file and convert to json
def excel_to_json(ex_file_path):
    df_clk = pd.read_excel(ex_file_path)
    df_json = df_clk.to_json(orient="records", indent=4, force_ascii=False)
    return json.loads(df_json)

# Function to write dataframe to excel
def df_to_excel(df, file_path, sheet):
    file = pathlib.Path(file_path)
    if file.exists() == False:
        with pd.ExcelWriter(file_path, mode='w', engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet, index=False)
    with pd.ExcelWriter(file_path, mode='a', engine="openpyxl", if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=sheet, index=False)

# Function to initialize the browser
def init_browser():
    options = Options()
    options.add_argument("--headless=new")
    browser = webdriver.Firefox(service=Service(executable_path=GeckoDriverManager().install()), options=options)
    return browser

# Function to login to the website
def login(browser, url, username, password):
    browser.get(url)
    browser.maximize_window()
    time.sleep(5)
    usernameInput = browser.find_element('name', 'frmLoginPanel:inpUser')
    passwordInput = browser.find_element('name', 'frmLoginPanel:inpPass')
    usernameInput.send_keys(username)
    passwordInput.send_keys(password)
    time.sleep(2)
    passwordInput.send_keys(Keys.ENTER)
    time.sleep(2)
    browser.get(url2)
    time.sleep(2)
    
def click_element(browser, el_xpath):
    el = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, el_xpath)))
    actions = ActionChains(browser)
    actions.click(el).perform()
    time.sleep(2)

# Function to gathering data from the website
def scrape_data(browser, dfbase, max_dates, table_xpath, elm_xpath_3):
    last_rec = datetime.now().replace(microsecond=0, second=0)
    try:        
        table =WebDriverWait(browser,10).until(EC.visibility_of_element_located((By.XPATH, table_xpath))).get_attribute("outerHTML")
        if not table:
          # code to execute if the table variable is null or empty
          last_rec = datetime.now()
          print("Table does not exist!............")
        else:
          df=pd.read_html(str(table),thousands = '.', decimal= ',')[0]
          df.Zaman = pd.to_datetime(df.Zaman)
          dfbase = pd.concat([dfbase, df], axis=0)
          last_rec = dfbase.Zaman.tail(1).item()

        while last_rec > max_dates:
            try:
                click_element(browser, elm_xpath_3)
                table =WebDriverWait(browser,10).until(EC.visibility_of_element_located((By.XPATH, table_xpath))).get_attribute("outerHTML")
                time.sleep(2)
                df=pd.read_html(str(table),thousands = '.', decimal= ',')[0]
                df.Zaman = pd.to_datetime(df.Zaman)
                dfbase = pd.concat([dfbase, df], axis=0)
                last_rec = dfbase.Zaman.tail(1).item()
            except Exception as e:
                print(e)

    except Exception as e:
        print(e)
        print("end of While!............")
        browser.quit()

    return dfbase

# Initialize a list of JSON objects containing login credentials
clklist = excel_to_json(firm_list)

# Iterate through the list of login credentials
for i in clklist:
    print("Started 1.!............")

    try:
        dfbase=pd.DataFrame()
        Hizmet_Noktası_No = i['Hizmet_Noktası_No']
        Sayaç_Seri_No = i['Sayaç_Seri_No']
        Tesis_ID =i['Tesis_ID']
        username = i['username']
        password = i['password']

        try:
            browser = init_browser()
            login(browser, url, username, password)
            click_element(browser, elm_xpath_1)
            click_element(browser, elm_xpath_2)  

        except Exception as e:
            print(e)
            browser.quit()
            temp_error = {'username': username,'password': password}
            error_dict.append(temp_error) 
            print("Browser Error!............")

        ssn_values = [Sayaç_Seri_No]
        if df_sql[df_sql['ssno'].isin(ssn_values)].empty:
            max_dates = pd.to_datetime('today') - pd.offsets.MonthEnd()
        else:
            max_dates = df_sql[df_sql['ssno'].isin(ssn_values)]['date'].max()

        last_rec = datetime.now().replace(microsecond=0, second=0)

        dfbase = scrape_data(browser, dfbase, max_dates, table_xpath, elm_xpath_3)

        browser.quit()

        print("Dfbase Started!............")

        dfbase = dfbase[dfbase["Zaman"] > max_dates]

        dfbase['username'] = username
        dfbase['Hizmet_Noktası_No'] = Hizmet_Noktası_No
        dfbase['Sayaç_Seri_No'] = Sayaç_Seri_No

        dfbase = dfbase[dfbase['Zaman'].dt.minute == 0]
        dfbase['facility_id'] = Tesis_ID

        dfall = pd.concat([dfall, dfbase], axis=0)
    except:
        temp_error = {'username': username,'password': password}
        error_dict.append(temp_error)
        
# Preparing the data and save MySQL database
try:
    dfall = dfall.rename(columns={
        'Zaman': 'date', 
        'Aktif Enerji(kWh)': 'active',
        'Endüktif Tüketim Ri(kVArh)': 'inductive', 
        'Kapasitif Tüketim Rc(kVArh)': 'capacitive',
        'Hizmet_Noktası_No': 'hno', 
        'Sayaç_Seri_No': 'ssno' })
    dfall = dfall[['date', 'active', 'inductive', 'capacitive', 'hno', 'ssno', 'facility_id']]
    dfall.dropna(inplace=True)
    dfall.to_sql(name="consumptions" ,con=engine,index=False, if_exists='append')
    print('Sucessfully written to Remote Database!!!')
except Exception as e:
    print(e)
    
df_to_excel(dfall,data_path, 'dfall')

df_error = pd.DataFrame(error_dict)
df_error = df_error.drop_duplicates(keep='first')
df_to_excel(df_error,data_path, 'error')
