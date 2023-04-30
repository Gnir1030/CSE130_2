import re
import os

file_names = ["shrek_script.txt", "shrek_2_script.txt"]

word_count_dict = {}

for file_name in file_names:
    f = open(file_name, 'r')
    text = f.read().replace('_', ' ')
    words = re.split( r"\W+", text)
    for word in words:
        word = word.lower()
        if word in word_count_dict.keys():
            word_count_dict[word] += 1
            continue
        word_count_dict[word] = 1

os.system("make")
os.system("./word-count 10 10 shrek_script.txt shrek_2_script.txt > shrek_c_out.txt")

f = open("shrek_c_out.txt", "r")

lines = f.readlines()

for line in lines:
    try:
        pair = line.split(",")
        assert word_count_dict[pair[0]] == int(pair[1])
    except AssertionError:
        print(f"Word: {pair[0]}")
        print(f"C Val: {pair[1]}")
        print(f"Py Val: {word_count_dict[pair[0]]}")

f.close()