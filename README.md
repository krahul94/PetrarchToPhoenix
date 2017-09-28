# PetrarchToPhoenix
# How to execute
Python file petrarchToPhoenix is executed using Python 2.7 interpreter.
# Parameters to be given
When the program executes the user is prompted to give locations of source file and destination file respectively. Also the user is
prompted to enter the url and port number for Clavin Cliff server in order to get access to geo location services.

# Output
The program produces a collection where each document consists of the following fields.
```
 {'event_id': , 'date8': , 'year': , 'month': , 'day': ,
'source': , 'src_actor': , 'src_agent': , 'src_other_agent': ,
'target': , 'tgt_actor': , 'tgt_agent': , 'tgt_other_agent': ,
'code': , 'root_code': , 'quad_class': , 'goldstein': ,
'geoname': , 'country_code': , 'admin_info': , 'id': ,  'url': ,
'source_text': , 'longitude': , 'latitude': }
```

# How it works
Each document of the input file is processed and values for each field are extracted. Fields pertaining to location such as geoname, 
country_code, longitude, latitude are extracted with the help of Clavin Cliff server. 

# Methods involved
process_cameo():  This method is used for finding values for code, root_code, quad_class and goldstein fields.

process_actors():  This method is used for finding source, target and actors corresponding to the event.

geoLocation():  This method is used for finding the location at which the event occurred.
