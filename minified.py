import json 

with open("merged_results.json", "r") as file_in: 
    content = json.load(file_in)


with open("merged_results-minified.json", "w") as file_out: 
    json.dump(content, file_out, separators=(',', ':'))
