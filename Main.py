__author__ = 'adamidesa'

import Parser
import Indices
import test
import sys
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import LeaveOneOut
from sklearn.metrics import accuracy_score
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC


trainset = sys.argv[1]
predictset = sys.argv[2]


def predictions(algo,vectors,labels,clfLogisticRegression):
    X=np.array(vectors)
    y=np.array(labels)

    observed = []
    predicted =[]
    scores= []

    loo = LeaveOneOut(len(vectors))
    for train_index, test_index in loo:
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]
        if algo =='Random Forest':
            clf = RandomForestClassifier(n_estimators=100, oob_score=True)
            clf = clf.fit(X_train,y_train)
            observed.append(y_test[0])
            predicted.append(clf.predict(X_test)[0])
            scores.append(clf.oob_score_)
        elif algo == 'Naive Bayes':
            clf = MultinomialNB()
            clf = clf.fit(X_train,y_train)
            observed.append(y_test[0])
            #print(X_train, X_test, y_train, y_test)
            predicted.append(clf.predict(X_test)[0])
            #scores.append(clf.oob_score_)
        elif algo == 'Support Vector Machines':
            clf = SVC(kernel='linear')#kernel
            clf = clf.fit(X_train,y_train)
            #print(X_train)
            observed.append(y_test[0])
            predicted.append(clf.predict(X_test)[0])
            #scores.append(clf.oob_score_)
            #print(X_train, X_test, y_train, y_test)
        elif algo == 'Logistic Regression':
            clfLogisticRegression = clfLogisticRegression.fit(X_train,y_train)
            #print(X_train)
            observed.append(y_test[0])
            predicted.append(clfLogisticRegression.predict(X_test)[0])
            #scores.append(clf.oob_score_)
            #print(X_train, X_test, y_train, y_test)
    accuracy = accuracy_score(observed, predicted)
    return accuracy,clfLogisticRegression

if __name__ == "__main__":
    print('*******Starting program******')

    print('Number of arguments:', len(sys.argv))
    print('Argument List:', str(sys.argv))
    clfLogisticRegression = LogisticRegression()
    histFeatures = ['goals','shots','corners']
    #Indices.updateIndices(histFeatures)
    #histFeatures = ['goals','shots','fouls','corners','yellow','red']

    vectors,labels,predictVectors = Parser.parsefile(trainset,histFeatures,predictset)

    #algos = ['Naive Bayes','Support Vector Machines','Logistic Regression','Random Forest']
    algos = ['Logistic Regression']

    #exit(0)
    for i in range(0,len(algos)):
        accuracy,clfLogisticRegression=predictions(algos[i],vectors,labels,clfLogisticRegression)
        print(algos[i]+": "+str(accuracy))

    for upcomingMatch in predictVectors:
        print(clfLogisticRegression.predict(upcomingMatch))








