//This code scrapes family search for pids
python:
from sfi import Macro
import os
import sys
sys.path.append(r'R:\JoePriceResearch\Python\all_code')
from FamilySearch1 import FamilySearch

#specify parameters for FamilySearch object
FSusername = '***********'
FSpassword = "*********"
directory = Macro.getGlobal('directory')
os.chdir(directory)
inputfile = 'temp_arks_to_scrape.csv'
outputfile = 'temp_pids_scraped.csv'

#inputfile must have only two columns, first column is a unique "index", second column contains your arks (column header doesn't matter)
fs = FamilySearch(FSusername, FSpassword, directory, inputfile, outputfile, auth=True)
fs.GetPidFromArk()
end

