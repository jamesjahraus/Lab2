# West Nile Outbreak Simulation
> Final project for GIS 305 at FRCC (fictitious simulation)
> 
> James Jahraus

The West Nile Outbreak Simulation is a tool used to determine the Boulder addresses that require pesticide spraying.
The intersection of Boulder Lakes_and_Reservoirs, Mosquito_Larval_Sites, OSMP_Properties, and Wetlands_Regulatory
are used to determine the addresses at risk for West Nile Virus.
Addresses in the **At Risk Zone** will require pesticide spraying.
Citizens may opt out for health conditions, so the avoid_points buffer **Avoid Zone** is erased from the **At Risk Zone**.
This is the final at risk area that will require pesticide spraying.
All addresses are added to this zone to produce the final Target_Addresses.


### West Nile Outbreak Simulation Tasks 


* run_etl 
  * Run etl, generates the avoid_points feature class.


* run_analysis
  * Run Analysis to create the final analysis features.


* render_layout
  * Render the map including analysis features, correct colours, subtitle, and addresses at risk count.


* generate_report
  * Generate a csv report in the WestNileOutbreak directory with the Target Addresses that require spraying.
    

### Run `finalproject.py`


* Clone this repository


* Include ArcGIS Pro project WestNileOutbreak
  * Contains WestNileOutbreak.gdb, and WestNileOutbreak_Outputs.gdb


* Setup local parameters in `wnvoutbreak.yaml`   


* run `finalproject.py`
  * Use input gui for intersect analysis feature class name, buffer distance, and map subtitle.
  

* Expected outputs:
  * `./WestNileOutbreak/wnv.log` contains an execution log of all the functions or errors. 
  * `./WestNileOutbreak/wnv.pdf` contains a map of the final feature classes:
    * `./WestNileOutbreak/WestNileOutbreak_Outputs.gdb/avoid_points_buf, final_analysis, Target_Addresses`
  * `./WestNileOutbreak/target_addresses.csv` contains a csv of all the addresses that require pesticide spraying.
  
  