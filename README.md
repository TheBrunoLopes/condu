# Condu a Python 3.6 client for Conductor
The previous client was written in python 2.7.


This client was built upon the previous work and extended with 
added functionalities and performance boosts.


Provides 3 sets of functions:

1. Workflow management APIs (start, terminate, get workflow status etc.)
2. Worker execution framework
3. Task and Workflow definitions

## Install

```
pip install condu
```

## Using Workflow Management API
Python class ```Condu``` provides client API calls to the conductor server to start manage the workflows.
