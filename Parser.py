__author__ = 'adamidesa'

import csv


def parsefile(file):
    print('*******Parsing csv file *******')

    print('*********' + file + '**********')

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
    # goals,shots,fouls,corners,yellow,red
    for i in range(0, len(teams)):
        teamstats.append([0, 0])
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
                    print('Home', hometeam)
                    totalmatchesperteam[i] += 1
                if awayteam == teams[i]:
                    print('Away', awayteam)
                    totalmatchesperteam[i] += 1
