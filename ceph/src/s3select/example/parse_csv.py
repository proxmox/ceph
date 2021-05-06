#!/usr/bin/python

import csv

with open('stam.txt') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        #if (int(row[0])==465 and int(row[5])==268): # casting is slower
        if (row[0]=="465" and row[5]=="268"):
            print row

