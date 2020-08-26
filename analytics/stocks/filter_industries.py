fd = open('industries.txt', 'r')
industries = []
for line in fd:
    industry = line.strip()
    if len(industry) > 0 and industry not in industries:
        industries += [industry]
fd.close()
fd = open('ind.txt', 'w')
i=0
for industry in industries:
    fd.write('"'+industry+'":'+str(i)+'\n')
    i+=1
fd.close()
