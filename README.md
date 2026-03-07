# Progress Tracker

Progress Tracker is a small golang project, that compiles to one single binary, allowing the user to move and execute it everywhere.  
The executive starts up a webserver with the address http://127.0.0.1:6401 where the user 
can then enter his data, which is saved to the corresponding data.sqlite file - which is always in the same folder as the binary.

## Features

The Application allows for creating of own categories. Every category can then
be edited and viewed.  
Categories can be amended, meaning columns can be deleted or
added.   
Every table automatically gets an ID column that can't be deleted.