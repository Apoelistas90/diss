__author__ = 'adamidesa'

import Parser
import sys
from sklearn.ensemble import RandomForestClassifier

historical = sys.argv[1]

if __name__ == "__main__":
    print('*******Starting program******')

    print('Number of arguments:', len(sys.argv))
    print('Argument List:', str(sys.argv))

    #print('*******Fetching input files*******')
    #print('*******Fetching 1st input file: ' + historical + ' *******')

    Parser.parsefile(historical)
    clf = RandomForestClassifier(n_estimators=100, oob_score=True)






