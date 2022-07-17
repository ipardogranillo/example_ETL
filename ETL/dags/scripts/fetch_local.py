import os
import re

def fetchCsv():
    print('getting most recent file')
    dirSearch = [d for d in os.listdir('.')
            if bool(re.match('^[0-9]+$', d))
            and len(d) == 8
            and os.path.isdir('./' + d)]
    if len(dirSearch) > 0:
        mostRecentDir = max(dirSearch)
        print(f'most recent folder from {mostRecentDir}')
        return mostRecentDir
    else:
        print('no names with YYYYMMDD format found')
        return None