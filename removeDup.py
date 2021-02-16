import csv

f = open("2021-01-01res.txt")
csv_data = csv.reader(f)
for row in csv_data:
    print(row)

'''with open('2021-01-01test.txt') as result:
    uniqlines = set(result.readlines())
    with open('2021-01-01res.txt', 'w') as rmdup:
        rmdup.writelines(set(uniqlines))'''