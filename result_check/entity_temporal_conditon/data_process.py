import json
import sys

with open('data_maria', 'r') as file:
	data = json.load(file)

result = data['result']['roads']

result.sort(key = lambda x: int(x))

with open('maria', 'w') as file:
	sys.stdout = file
	for ele in result:
		print(int(ele))



