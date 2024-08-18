## Case Study:
The following case study illustrates the application of the above concepts to a real-world scenario.

### Project Folder Structure
```
Car_Crash_Case_Study/
├── config/
│   └── config.yaml
├── resources/
│   ├── output/
│       ├── analysis-1
│       ├── analysis-2
│       ├── analysis-3
│       ├── analysis-4
│       ├── analysis-5
│       ├── analysis-6
│       ├── analysis-7
│       ├── analysis-8
│       ├── analysis-9
│       └── analysis-10
│   └── raw_data/
│       ├── Charges_use.csv
│       ├── Damages_use.csv
│       ├── Endorse_use.csv
│       ├── Primary_Person_use.csv
│       ├── Restrict_use.csv
│       └── Units_use.csv
├── src/
│   ├── _init_.py
│   ├── analysis.py
│   └── utils.py
├── main.py
└── README.md
```

### Analytics and Results:
Application should perform below analysis and store the results for each analysis.
Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
``` 
0 
```
Analysis 2: How many two wheelers are booked for crashes?
```
781
```
Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
```
['CHEVROLET', 'FORD', 'NISSAN', 'DODGE', 'HONDA']
```
Analysis 4: Determine number of Vehicles with driver having valid licenses involved in hit and run? 
``` 
2569
```
Analysis 5: Which state has highest number of accidents in which females are not involved? 
```
Texas
```
Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
```
['AMERICAN IRON HORSE', 'ACURA', 'WHITE', 'MOTOR COACH MND INC', 'PIERCE', 'PORSCHE', 'WHITEGMC', 'FREIGHTLINER', 'BUELL', 'STERLING', 'HYUNDAI', 'INTERNATIONAL', 'PETER PIRSCH & SONS', 'FIAT', 'NA', 'TOYOTA', 'RAM', 'SUBARU', 'NISSAN', 'IC CORPORATION', 'BENTLEY', 'EMERGENCY ONE', 'SUZUKI', 'INFINITI', 'HINO', 'FORD', 'AUDI', 'PREVOST', 'FERRARI', 'ISUZU', 'OLDSMOBILE', 'WESTERN TRUCK & TRAILER CO', 'MINI', 'SAAB', 'DODGE', 'CHEVROLET', 'JAGUAR', 'MCI (LES AUTO BUS)', 'MERCURY', 'LEXUS', 'NORTH AMERICAN BUS', 'JEEP', 'VOLVO', 'GRAYCO', 'JOHN DEERE', 'TESLA', 'DUCATI', 'GM', 'UNKNOWN', 'MAZDA', 'DAEWOO', 'REQUIRES INTERPRETATION', 'BMW', 'MASERATI', 'VOLKSWAGEN', 'GEO', 'PONTIAC', 'EAGLE', 'BLUE BIRD', 'HOMEMADE VEHICLE', 'ALL OTHER MAKES', 'HARLEY-DAVIDSON', 'YAMAHA', 'KIA', 'HUMMER', 'WESTERN STAR', 'HONDA', 'OTHER (EXPLAIN IN NARRATIVE)', 'SATURN', 'KAWASAKI', 'GMC', 'CRANE CARRIER', 'SMART', 'CADILLAC', 'ELDORADO', 'NEW FLYER', 'PLYMOUTH', 'LINCOLN', 'MACK', 'THOMAS', 'CATERPILLAR', 'MURPHY MANUFACTURING CO', 'MERCEDES-BENZ', 'BUICK', 'KENWORTH', 'AUTOCAR', 'CHRYSLER', 'AMERICAN MOTORS', 'LAND ROVER', 'MITSUBISHI', 'PETERBILT', 'UTILIMASTER', 'GILLIG', 'DATSUN', 'CAN-AM', 'COUNTRY COACH', 'FLOE INTERNATIONAL', 'BUGATTI', 'WINNEBAGO', 'SPARTAN', 'WORKHORSE', 'TRI-QUEST INC', 'RENAULT', 'PIAGGIO', 'ORION', 'HYUNDAI STEEL INDUSTRIES', 'NISSAN DIESEL', 'POLARIS', 'OTTAWA', 'AUTOCAR LLC', 'FORETRAVEL', 'INTERMOUNTAIN WHOLE SALE INC', 'VAN HOOL', 'APRILIA', 'NABORS TRAILERS', 'ROLLS-ROYCE', 'ROAD RAILER', 'VICTORY', 'BUS & COACH INTL', 'INTERNATIONAL TRAILER CORP', 'MIDLAND MANUFACTURING LIMITED', 'CHALLENGE-COOK BROTHERS INC', 'FLOW BOY MFG', 'KYMCO', 'INTERNATIONAL TANK & TRAILER CORP', 'FEDERAL MOTORS', 'VESPA', 'INDIAN', 'ASTON MARTIN', 'AM GENERAL', 'U-HAUL INTERNATIONAL', 'HYOSUNG', 'SUPREME CORP', 'GENERAL TRAILER CO', 'EAGLE TRANSIT', 'GALBREATH INC', 'ULTRA LITE MFG', 'NOVA FABRICATING', 'ALFA ROMEO', 'HEIL CO', 'EAGER BEAVER', 'HOGG/DAVIS INC', 'D & B TRAILER', 'MAC LTT', 'UNITED EXPRESS LINE INC', 'NOVA', 'FRUEHAUF', 'MCLAREN', 'MANAC INC', 'BRI-MAR MANUFACTURING LLC', 'INDEPENDENT TRAILER MFG', 'MITSUBISHI FUSO', 'STEPHENS PNEUMATIC', 'LOTUS', 'KENTUCKY MFG', 'WABASH NATIONAL CORP', 'SPARTA MANUFACTURING CORP', 'FEATHERLITE MFG INC']
```
Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
```
+---------------------------------+-----------------+
|VEH_BODY_STYL_ID                 |PRSN_ETHNICITY_ID|
+---------------------------------+-----------------+
|AMBULANCE                        |WHITE            |
|BUS                              |HISPANIC         |
|FARM EQUIPMENT                   |WHITE            |
|FIRE TRUCK                       |WHITE            |
|MOTORCYCLE                       |WHITE            |
|NA                               |WHITE            |
|NEV-NEIGHBORHOOD ELECTRIC VEHICLE|WHITE            |
|NOT REPORTED                     |HISPANIC         |
|OTHER  (EXPLAIN IN NARRATIVE)    |WHITE            |
|PASSENGER CAR, 2-DOOR            |WHITE            |
|PASSENGER CAR, 4-DOOR            |WHITE            |
|PICKUP                           |WHITE            |
|POLICE CAR/TRUCK                 |WHITE            |
|POLICE MOTORCYCLE                |HISPANIC         |
|SPORT UTILITY VEHICLE            |WHITE            |
|TRUCK                            |WHITE            |
|TRUCK TRACTOR                    |WHITE            |
|UNKNOWN                          |WHITE            |
|VAN                              |WHITE            |
|YELLOW SCHOOL BUS                |WHITE            |
+---------------------------------+-----------------+
```
Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
```
['76010', '75067', '78521', '77084', '78574']
```
Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
```
[14870169, 14894076, 14996273, 15232090, 15232090, 15249931, 15307513]
```
Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offenses, has licensed Drivers, used top 10 used vehicle colors and has car licensed with the Top 25 states with highest number of offenses. (to be deduced from the data)
```
['FORD', 'CHEVROLET', 'TOYOTA', 'DODGE', 'NISSAN']
```
