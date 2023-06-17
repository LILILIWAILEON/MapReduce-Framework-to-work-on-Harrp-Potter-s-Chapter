import re
from collections import Counter

def mapper(document):
    # Read stop words from the "stop_words.txt" file
    with open("NLP_stopWords.txt", "r", encoding="utf-8") as f:
        stop_words = set(f.read().split(','))

    # Extract words from the document and emit intermediate key-value pairs
    words = re.findall(r'\w+', document.lower())
    for word in words:
        if word not in stop_words and not word.isnumeric(): #only if the word is not in the set of stop words and is not a numeric value
            yield (word, 1)

def reducer(word, counts):
    # Aggregate the word counts for each word
    total = sum(counts)
    yield (word, total)

if __name__ == '__main__':
    # Read the input data from the "hp.txt" file
    with open("hp.txt", "r", encoding="utf-8") as f:
        data = f.read()

    # Map phase: apply the mapper function to each line of the input data
    intermediate = []
    for line in data.split('\n'):
        intermediate.extend(mapper(line))

    # Shuffle phase: group intermediate key-value pairs by key
    groups = {}
    for key, value in intermediate:
        if key in groups:
            groups[key].append(value)
        else:
            groups[key] = [value]

    # Reduce phase: apply the reducer function to each group of values with the same key
    output = []
    for key, values in groups.items():
        for result in reducer(key, values):
            output.append(result)

    # Sort the output by word count in descending order
    output.sort(key=lambda x: x[1], reverse=True)

    # Print the top 20 frequently occurring words
    for word, count in output[:20]:
        print(f'{word} {count}')