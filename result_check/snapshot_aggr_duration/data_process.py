import json
import sys

with open('data_pg', 'r') as file:
	data = json.load(file)

data = data['result']['roadStatDuration']

result = []

for ele in data:
	result.append((ele['left'], ele['middle'], ele['right']))

result.sort(key = lambda x: ((int(x[0])), x[1]))

with open('pg', 'w') as file:
	sys.stdout = file
	for ele in result:
		print(int(ele[0]), ele[1], ele[2])



