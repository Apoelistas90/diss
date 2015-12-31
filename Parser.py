__author__ = 'adamidesa'

import csv


def parsefile(train_file,test_file):
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
    totals = []
    teamstatsref = ['goals','shots','fouls','corners','yellow','red']
    vectors=[]
    labels=[]
    # goals,shots,fouls,corners,yellow,red
    for i in range(0, len(teams)):
        # teamstats.append([0.0, 0.0, 0.0, 0.0, 0.0 ,0.0])
        teamstats.append([0.0,0.0])
        totals.append([0.0,0.0])

    totalmatchesperteam = []
    for i in range(0, len(teams)):
        totalmatchesperteam.append(0)

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
                    teamstats[i]=computestats(match, totals[i],'home',totalmatchesperteam[i])
                    home=updatestats(match, teamstats[i],'home',totalmatchesperteam[i])
                    #if hometeam == 'Tottenham':
                        #print home
                if awayteam == teams[i]:
                    totalmatchesperteam[i] += 1
                    if awayteam == 'Tottenham':
                        print("1: ")
                        print teamstats[i]
                    teamstats[i]=computestats(match, totals[i],'away',totalmatchesperteam[i])
                    if awayteam == 'Tottenham':
                        print("2: ")
                        print teamstats[i]
                    away=updatestats(match, teamstats[i],'away',totalmatchesperteam[i])
                    if awayteam == 'Tottenham':
                        print("3: ")
                        print away

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

    print('Reference for team stats used: '+str(teamstatsref))
    for i in range(0,len(teams)):
        print(str(totalmatchesperteam[i])+ ' matches played'+ ' for '+teams[i]+' and final team stats are : '+str(teamstats[i]))


    vectors.pop(0)
    labels.pop(0)
    print(vectors)
    print(labels)



    #exit(0)

    return vectors,labels


def computestats(partition,team,edra,totalmatches):
    stat = [0.0, 0.0]
    if edra == 'home':
        team[0] += int(partition[4])
        stat[0] = team[0]/totalmatches#goals
        team[1] += int(partition[12])
        stat[1] = team[1] /totalmatches#shots
        #team[2] = team[2] + int(partition[14])#fouls
        #team[2] = team[2] + int(partition[16])#corners
        #team[4] = team[4] + int(partition[18])#yellow
        #team[5] = team[5] + int(partition[20])#red
        return stat
    elif edra == 'away':
        team[0] += int(partition[5])
        stat[0] = team[0]/totalmatches#goals
        team[1] += int(partition[13])
        stat[1] = team[1]/totalmatches#shots
        #team[2] = team[2] + int(partition[15])#fouls
        #team[2] = team[2] + int(partition[17])
        #team[4] = team[4] + int(partition[19])
        #team[5] = team[5] + int(partition[21])
        return stat
# normalise
def updatestats(partition,team,edra,totalmatches):
    home = [0.0, 0.0]
    away = [0.0, 0.0]

    if edra == 'home':
        home[0] = ((team[0]*totalmatches) + int(partition[4]))/(totalmatches+1)#goals
        home[1] = ((team[1]*totalmatches) + int(partition[12]))/(totalmatches+1)#shots
        #team[2] = ((team[2]*totalmatches) + int(partition[14]))/(totalmatches+1)#fouls
        #team[2] = ((team[2]*totalmatches) + int(partition[16]))/(totalmatches+1)
        #team[4] = ((team[4]*totalmatches) + int(partition[18]))/(totalmatches+1)
        #team[5] = ((team[5]*totalmatches) + int(partition[20]))/(totalmatches+1)
        return home
    elif edra == 'away':
        away[0] = ((team[0]*totalmatches) + int(partition[5]))/(totalmatches+1)#goals
        away[1] = ((team[1]*totalmatches) + int(partition[13]))/(totalmatches+1)#shots
        #team[2] = ((team[2]*totalmatches) + int(partition[15]))/(totalmatches+1)#fouls
        #team[2] = ((team[2]*totalmatches) + int(partition[17]))/(totalmatches+1)
        #team[4] = ((team[4]*totalmatches) + int(partition[19]))/(totalmatches+1)
        #team[5] = ((team[5]*totalmatches) + int(partition[21]))/(totalmatches+1)
        return away
