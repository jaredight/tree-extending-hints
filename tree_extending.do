/*
Jared Wright
jaredwright217@gmail.com
5 March 2021
Tree-extending hints and add families
Please make a copy if you change anything except the global variables
This code works for 1900 - 1940.
*/

clear all
set more off

* define global variables
global place ""
global county "Nodaway"
global state "Missouri"
global new_folder = "Nodaway_Missouri"
global years 1900 1910 1920 // list of all years for which you need tree extending hints


* create a new folder to store hints
global directory "V:\FHSS-JoePriceResearch\tools\community_reconstruction\tree_extending_add_family\"
capture mkdir "${directory}\${new_folder}"
global directory "${directory}\${new_folder}"
cd "$directory"

foreach year of global years {
	clear all
	global year = `year'

	* get all the people in a particular place and census year
	cd V:\FHSS-JoePriceResearch\data\census_refined\fs\/$year
	use "ark${year}_event_state" if strpos(event_state, "$state") > 0, clear
	merge 1:m ark${year} using "ark${year}_event_county", nogen keep(1 3)
	keep if strpos(event_county, "$county") > 0
	sort ark${year}
	drop if ark${year}==ark${year}[_n-1]

	if "${place}" != "" {
		preserve
		*This code searches the crosswalk for what the city was called in earlier census records. Do not recommend keeping only exact place matches because place names tend to change quite a bit across census years.
		cd V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\anc_cities_crosswalk
		use anc_city_crosswalk_years_1900-1940, clear
		keep if strpos(state1940, "$state") > 0
		gen ucounty = upper("${county}")
		display "`ucounty'"
		keep if strpos(county1930, ucounty) > 0

		gen uplace = upper("${place}")
		keep if strpos(city1940, uplace) > 0
		gen citycounty = city$year + "---" + county$year
		levelsof citycounty, local(historicplaces)
		restore
		
		cd V:\FHSS-JoePriceResearch\data\census_refined\fs\/$year
		merge 1:m ark${year} using "ark${year}_event_place", nogen keep(1 3)
		generate byte keepplace = 0
		generate event_place_caps = upper(event_place)
		generate event_county_caps = upper(event_county)
		foreach hplace in `historicplaces' {
			display "`hplace'"
			local pos = strpos("`hplace'", "---")
			local cityname = substr("`hplace'", 1, `pos' - 1)
			di "`cityname'"
			local countyname = substr("`hplace'", `pos' + 3, .)
			di "`countyname'"
			replace keepplace = 1 if (strpos(event_place_caps, "`cityname'") > 0) & (strpos(event_county_caps, "`countyname'") > 0)
		}
		replace keepplace = 1 if strpos(event_place, "$place") > 0
		keep if keepplace
		drop keepplace event_place_caps event_county_caps
		
		sort ark${year}
		drop if ark${year}==ark${year}[_n-1]
	}

	* merge in record ids
	merge 1:1 ark${year} using V:\FHSS-JoePriceResearch\data\census_refined\fs\/${year}\ark${year}_record_id, nogen keep(1 3)
	drop if missing(record_id)

	* Drop unknown, boarder, employee, inmate, religious, patient, military, student
	merge 1:1 ark${year} using R:\JoePriceResearch\record_linking\data\census_compact\/${year}\census${year}, nogen keep(1 3)
	drop if inlist(rel,-1,28,29,34,35,36,39,40)

	* save temp file
	cd "$directory"
	save temp, replace

	* prepare arks for scraping
	rename ark${year} ark
	gen index = _n
	export delim index ark using "temp_arks_to_scrape", replace

	* scrape arks for pids
	cd "V:\FHSS-JoePriceResearch\tools\community_reconstruction\tree_extending_add_family"
	do "python_scraper.do"

	* merge in scraped pids
	cd "$directory"
	preserve
	import delimited "temp_pids_scraped.csv", varnames(1) clear
	save "temp_pids_scraped", replace
	restore
	merge 1:m ark using "temp_pids_scraped", nogen update keep(1 3 4 5)
	duplicates drop ark, force
	rename ark ark${year}

	* merge family search hints
	sort ark${year}
	drop if ark${year}==ark${year}[_n-1]
	merge 1:m ark${year} using V:\FHSS-JoePriceResearch\data\census_refined\fs\fs_hints\ark${year}_pid_hints, nogen update keep(1 3 4 5)
	sort ark${year}
	drop if ark${year}==ark${year}[_n-1]

	* save temp file
	cd "$directory"
	save temp, replace

	** TREE-EXTENDING HINTS
	* Keep families with (a) 1 or more ark with no pid and (b) 1 or more ark with a pid 
	gen pid_present = !missing(pid)
	bys record_id: egen family_pids_count = sum(pid_present)
	gen unit = 1
	bys record_id: egen family_arks_count = sum(unit)

	keep if !missing(family_pids_count) & !missing(family_arks_count) & ///
		family_pids_count > 0 & ///at least one pid
		family_arks_count > family_pids_count //at least one missing pid

	* Keep only one pid for each family
	keep if !missing(pid)
	sort record_id rel //likely to keep the household head
	drop if record_id==record_id[_n-1]

	* Save file
	keep ark pid
	order ark
	compress
	local place = subinstr("${place}"," ","_",.)
	export delimited "tree_ext_`place'_${state}_${year}", replace

}

* append tree extending hints for all years
clear all
cd "$directory"
local place = subinstr("${place}"," ","_",.)
foreach year of global years {
    global year = `year'
	preserve
	import delimited "tree_ext_`place'_${state}_${year}", varnames(1) clear
	rename ark ark
	save temp, replace
	restore
	append using temp
	//erase "tree_ext_`place'_${state}_${year}"
}

* randomize hints and export links to a CSV
gen rand = runiform()
sort rand
gen url = "https://www.familysearch.org/search/linker?pal=/ark:/61903/1:1:" + ark + "&id=" + pid + "&cid=byurll"
keep url
export delimited "tree_ext_`place'_`state'_all ", replace

* erase temp files
cd "$directory"
capture erase "temp.dta"
capture erase "temp_arks_to_scrape.csv"
capture erase "temp_pids_scraped.csv"
capture erase "temp_pids_scraped.dta"
