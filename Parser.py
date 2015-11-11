__author__ = 'adamidesa'

import csv


def parsefile(file):
    teams = []
    teams.append('Arsenal')
    teams.append('Aston Villa')
    teams.append('Burnley')
    teams.append('Chelsea')
    teams.append('Crystal Palace')
    teams.append('Everton')
    teams.append('Hull')
    teams.append('Leicester')
    teams.append('Liverpool')
    teams.append('Man City')
    teams.append('Man United')
    teams.append('Newcastle')
    teams.append('Southampton')
    teams.append('Stoke')
    teams.append('Sunderland')
    teams.append('Swansea')
    teams.append('Tottenham')
    teams.append('QPR')
    teams.append('West Brom')
    teams.append('West Ham')

    # Features
    teamstats = []
    vectors=[]
    labels=[]
    # goals,shots,fouls,corners,yellow,red
    for i in range(0, len(teams)):
        teamstats.append([0, 0, 0, 0, 0 ,0])
        # teamstats.append([0])#goals,shots,fouls,corners,yellow,red

    totalmatchesperteam = []
    for i in range(0, len(teams)):
        totalmatchesperteam.append(0)

    with open(file, 'r') as csvfile:
        matches = csv.reader(csvfile, delimiter=',', quotechar='|')
        for match in matches:
            hometeam = match[2]
            awayteam = match[3]
            for i in range(0, len(teams)):
                if hometeam == teams[i]:
                    computestats(match, teamstats[i],'home')
                    totalmatchesperteam[i] += 1
                if awayteam == teams[i]:
                    computestats(match, teamstats[i],'away')
                    totalmatchesperteam[i] += 1

    #print (teams)
    #print (teamstats)

    for i in range(0,len(teams)):
        computeaverages(teamstats[i], totalmatchesperteam[i])



    #Construct Actual Instances and aggregate at each step
    with open(file,'r') as csvfile:
        matches = csv.reader(csvfile, delimiter=',', quotechar='|')
        #For each match
        k=0
        for match in matches:
            hometeam = match[2]
            awayteam = match[3]
            home=[]
            away=[]
            for i in range(0,len(teams)):
                if hometeam == teams[i]:
                    home = teamstats[i]# + teamother[i]
                    #home = teamother[i]
                    updatestats(match, teamstats[i],'home',totalmatchesperteam[i])
                    print(teams[i],home,'home')
                    totalmatchesperteam[i]+=1
                if awayteam == teams[i]:
                    away = teamstats[i]#+ teamother[i]
                    #away = teamother[i]
                    updatestats(match, teamstats[i],'away',totalmatchesperteam[i])
                    print(teams[i],away,'away')
                    totalmatchesperteam[i]+=1
            #print(home+away)
            vectors.append(home + away)
            #Get Label
            response = match[6]
            if response == 'H':
                response = 1
            elif response == 'A':
                response = 2
            elif response =='D':
                response = 0
            labels.append(response)

            #Aggregated until the last game
    for i in range(0,len(teams)):
        print(i, ' : total Matches', totalmatchesperteam[i],teams[i],teamstats[i])

    vectors.pop(0)
    labels.pop(0)
    print(vectors)
    print(labels)


def computestats(partition,team,edra):
    if edra == 'home':
        team[0] = team[0] + int(partition[4])#goals
        team[1] = team[1] + int(partition[12])#shots
        team[2] = team[2] + int(partition[14])#fouls
        team[3] = team[3] + int(partition[16])#corners
        team[4] = team[4] + int(partition[18])#yellow
        team[5] = team[5] + int(partition[20])#red

    elif edra == 'away':
        team[0] = team[0] + int(partition[5])#goals
        team[1] = team[1] + int(partition[13])#shots
        team[2] = team[2] + int(partition[15])#fouls
        team[3] = team[3] + int(partition[17])
        team[4] = team[4] + int(partition[19])
        team[5] = team[5] + int(partition[21])

def computeaverages(team,total):
    #print(team,total)
    team[0] = team[0] / total#goals
    team[1] = team[1] / total#shots
    team[2] = team[2] / total#fouls
    team[3] = team[3] / total
    team[4] = team[4] / total
    team[5] = team[5] / total
    #print(team)

def updatestats(partition,team,edra,totalmatches):
    if edra == 'home':
        team[0] = ((team[0]*totalmatches) + int(partition[4]))/(totalmatches+1)#goals
        team[1] = ((team[1]*totalmatches) + int(partition[12]))/(totalmatches+1)#shots
        team[2] = ((team[2]*totalmatches) + int(partition[14]))/(totalmatches+1)#fouls
        team[3] = ((team[3]*totalmatches) + int(partition[16]))/(totalmatches+1)
        team[4] = ((team[4]*totalmatches) + int(partition[18]))/(totalmatches+1)
        team[5] = ((team[5]*totalmatches) + int(partition[20]))/(totalmatches+1)
    elif edra == 'away':
        team[0] = ((team[0]*totalmatches) + int(partition[5]))/(totalmatches+1)#goals
        team[1] = ((team[1]*totalmatches) + int(partition[13]))/(totalmatches+1)#shots
        team[2] = ((team[2]*totalmatches) + int(partition[15]))/(totalmatches+1)#fouls
        team[3] = ((team[3]*totalmatches) + int(partition[17]))/(totalmatches+1)
        team[4] = ((team[4]*totalmatches) + int(partition[19]))/(totalmatches+1)
        team[5] = ((team[5]*totalmatches) + int(partition[21]))/(totalmatches+1)