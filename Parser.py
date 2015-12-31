import csv
import numpy as np
import Indices
__author__ = 'adamidesa'

TOTAL_TEAMS = 20
TOTAL_MATCHES = 10

def parsefile(train_file,histFeatures,predictset):
    teams=['Arsenal',
           'Aston Villa',
           'Chelsea',
           'Crystal Palace',
           'Everton',
           'Leicester',
           'Liverpool',
           'Man City',
           'Man United',
           'Newcastle',
           'Southampton',
           'Stoke',
           'Sunderland',
           'Swansea',
           'Tottenham',
           'West Brom',
           'West Ham']

    if train_file == 'prem_league_14_15_train.csv':
        teams.append('Burnley')
        teams.append('Hull')
        teams.append('QPR')
    elif train_file == 'prem_league_15_16_train.csv':
        teams.append('Bournemouth')
        teams.append('Watford')
        teams.append('Norwich')


    # Features
    teamstats = []
    totals = []
    vectors=[]
    labels=[]
    totalmatchesperteam = []

    for i in range(0, len(teams)):
        totalmatchesperteam.append(0)

    teamstats= np.zeros((TOTAL_TEAMS,len(histFeatures)))
    totals= np.zeros((TOTAL_TEAMS,len(histFeatures)))

    with open(train_file, 'r') as csvfile:
        matches = csv.reader(csvfile, delimiter=',', quotechar='|')
        # For each match, construct an input vector
        for match in matches:
            hometeam = match[2]
            awayteam = match[3]
            home = []
            away = []
            for i in range(0, len(teams)):
                if hometeam == teams[i]:
                    totalmatchesperteam[i] += 1
                    teamstats[i]=computestats(match, totals[i],'home',totalmatchesperteam[i],histFeatures)
                    home=updatestats(match, teamstats[i],'home',totalmatchesperteam[i],histFeatures)
                if awayteam == teams[i]:
                    totalmatchesperteam[i] += 1
                    teamstats[i]=computestats(match, totals[i],'away',totalmatchesperteam[i],histFeatures)
                    away=updatestats(match, teamstats[i],'away',totalmatchesperteam[i],histFeatures)

            vectors.append(np.append(home,away))

            # Get Label
            response = match[Indices.F_MATCH_RESULT]
            if response == 'H':
                response = 1
            elif response == 'A':
                response = 2
            elif response =='D':
                response = 0
            labels.append(response)

    print('Reference for team stats used: '+str(histFeatures))
    for i in range(0,len(teams)):
        print(str(totalmatchesperteam[i])+ ' matches played'+ ' for '+teams[i]+' and final team stats are : '+str(teamstats[i]))

    # Remove header
    vectors.pop(0)
    labels.pop(0)
    #print(vectors)
    #print(labels)

    predictVectors = parsePredictSet(predictset,teamstats,teams,histFeatures)

    print(predictVectors)

    return vectors,labels,predictVectors


def computestats(partition,team,edra,totalmatches,histFeatures):
    stat = np.zeros((len(histFeatures)))
    if edra == 'home':
        if 'goals' in histFeatures:
            team[Indices.GOALS_INDEX] += int(partition[Indices.F_H_GOALS_INDEX])
            stat[Indices.GOALS_INDEX] = team[Indices.GOALS_INDEX]/totalmatches
        if 'shots' in histFeatures:
            team[Indices.SHOTS_INDEX] += int(partition[Indices.F_H_SHOTS_INDEX])
            stat[Indices.SHOTS_INDEX] = team[Indices.SHOTS_INDEX]/totalmatches
        if 'fouls' in histFeatures:
            team[Indices.FOULS_INDEX] += int(partition[Indices.F_H_FOULS_INDEX])
            stat[Indices.FOULS_INDEX] = team[Indices.FOULS_INDEX]/totalmatches
        if 'corners' in histFeatures:
            team[Indices.CORNERS_INDEX] += int(partition[Indices.F_H_CORNERS_INDEX])
            stat[Indices.CORNERS_INDEX] = team[Indices.CORNERS_INDEX]/totalmatches
        if 'yellow' in histFeatures:
            team[Indices.YELLOW_INDEX] += int(partition[Indices.F_H_YELLOW_INDEX])
            stat[Indices.YELLOW_INDEX] = team[Indices.YELLOW_INDEX]/totalmatches
        if 'red' in histFeatures:
            team[Indices.RED_INDEX] += int(partition[Indices.F_H_RED_INDEX])
            stat[Indices.RED_INDEX] = team[Indices.RED_INDEX]/totalmatches
        return stat
    elif edra == 'away':
        if 'goals' in histFeatures:
            team[Indices.GOALS_INDEX] += int(partition[Indices.F_A_GOALS_INDEX])
            stat[Indices.GOALS_INDEX] = team[Indices.GOALS_INDEX]/totalmatches
        if 'shots' in histFeatures:
            team[Indices.SHOTS_INDEX] += int(partition[Indices.F_A_SHOTS_INDEX])
            stat[Indices.SHOTS_INDEX] = team[Indices.SHOTS_INDEX]/totalmatches
        if 'fouls' in histFeatures:
            team[Indices.FOULS_INDEX] += int(partition[Indices.F_A_FOULS_INDEX])
            stat[Indices.FOULS_INDEX] = team[Indices.FOULS_INDEX]/totalmatches
        if 'corners' in histFeatures:
            team[Indices.CORNERS_INDEX] += int(partition[Indices.F_A_CORNERS_INDEX])
            stat[Indices.CORNERS_INDEX] = team[Indices.CORNERS_INDEX]/totalmatches
        if 'yellow' in histFeatures:
            team[Indices.YELLOW_INDEX] += int(partition[Indices.F_A_YELLOW_INDEX])
            stat[Indices.YELLOW_INDEX] = team[Indices.YELLOW_INDEX]/totalmatches
        if 'red' in histFeatures:
            team[Indices.RED_INDEX] += int(partition[Indices.F_A_RED_INDEX])
            stat[Indices.RED_INDEX] = team[Indices.RED_INDEX]/totalmatches
        return stat

# Normalise
def updatestats(partition,team,edra,totalmatches,histFeatures):
    stat = np.zeros((len(histFeatures)))
    if edra == 'home':
        if 'goals' in histFeatures:
            stat[Indices.GOALS_INDEX] = ((team[Indices.GOALS_INDEX]*totalmatches) + int(partition[Indices.F_H_GOALS_INDEX]))/(totalmatches+1)
        if 'shots' in histFeatures:
            stat[Indices.SHOTS_INDEX] = ((team[Indices.SHOTS_INDEX]*totalmatches) + int(partition[Indices.F_H_SHOTS_INDEX]))/(totalmatches+1)
        if 'fouls' in histFeatures:
            stat[Indices.FOULS_INDEX] = ((team[Indices.FOULS_INDEX]*totalmatches) + int(partition[Indices.F_H_FOULS_INDEX]))/(totalmatches+1)
        if 'corners' in histFeatures:
            stat[Indices.CORNERS_INDEX] = ((team[Indices.CORNERS_INDEX]*totalmatches) + int(partition[Indices.F_H_CORNERS_INDEX]))/(totalmatches+1)
        if 'yellow' in histFeatures:
            stat[Indices.YELLOW_INDEX] = ((team[Indices.YELLOW_INDEX]*totalmatches) + int(partition[Indices.F_H_YELLOW_INDEX]))/(totalmatches+1)
        if 'red' in histFeatures:
            stat[Indices.RED_INDEX] = ((team[Indices.RED_INDEX]*totalmatches) + int(partition[Indices.F_H_RED_INDEX]))/(totalmatches+1)
        return stat
    elif edra == 'away':
        if 'goals' in histFeatures:
            stat[Indices.GOALS_INDEX] = ((team[Indices.GOALS_INDEX]*totalmatches) + int(partition[Indices.F_A_GOALS_INDEX]))/(totalmatches+1)
        if 'shots' in histFeatures:
            stat[Indices.SHOTS_INDEX] = ((team[Indices.SHOTS_INDEX]*totalmatches) + int(partition[Indices.F_A_SHOTS_INDEX]))/(totalmatches+1)
        if 'fouls' in histFeatures:
            stat[Indices.FOULS_INDEX] = ((team[Indices.FOULS_INDEX]*totalmatches) + int(partition[Indices.F_A_FOULS_INDEX]))/(totalmatches+1)
        if 'corners' in histFeatures:
            stat[Indices.CORNERS_INDEX] = ((team[Indices.CORNERS_INDEX]*totalmatches) + int(partition[Indices.F_A_CORNERS_INDEX]))/(totalmatches+1)
        if 'yellow' in histFeatures:
            stat[Indices.YELLOW_INDEX] = ((team[Indices.YELLOW_INDEX]*totalmatches) + int(partition[Indices.F_A_YELLOW_INDEX]))/(totalmatches+1)
        if 'red' in histFeatures:
            stat[Indices.RED_INDEX] = ((team[Indices.RED_INDEX]*totalmatches) + int(partition[Indices.F_A_RED_INDEX]))/(totalmatches+1)
        return stat

def parsePredictSet(predictFile,teamStats,teams,histFeatures):
    predictHomeVector = np.zeros((len(histFeatures)))
    predictAwayVector = np.zeros((len(histFeatures)))
    resultSet=np.zeros((TOTAL_MATCHES,(len(histFeatures)*2)))
    with open(predictFile, 'r') as csvfile:
        matches = csv.reader(csvfile, delimiter=',', quotechar='|')
        # For each match, construct an input vector
        MATCH=0
        for match in matches:
            hometeam = match[0]
            awayteam = match[1]
            for i in range(0, len(teams)):
                if hometeam == teams[i]:
                    predictHomeVector=teamStats[i]
                if awayteam == teams[i]:
                    predictAwayVector=teamStats[i]
            resultSet[MATCH]=np.append(predictHomeVector,predictAwayVector)
            MATCH+=1


    return resultSet