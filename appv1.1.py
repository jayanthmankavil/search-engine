from flask import Flask, request, render_template
import sqlite3
from urllib.parse import urlparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import heapq
from collections import defaultdict
import threading

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('crawler.db')
    conn.row_factory = sqlite3.Row
    return conn

def get_db_cursor(connection):
    return connection.cursor()

def retrieve_search_results(query):
    query = query.lower()

    query_words = query.split()

    db_connection = get_db_connection()
    db_cursor = get_db_cursor(db_connection)

    search_results = defaultdict(list)
    for word in query_words:
        db_cursor.execute('SELECT url FROM indexed_data WHERE word = ?', (word,))
        results = db_cursor.fetchall()
        for result in results:
            url = result['url']
            search_results[url].append(word)

    formatted_results = rank_search_results(search_results, query, db_cursor)

    db_cursor.close()
    db_connection.close()

    return formatted_results

def rank_search_results(search_results, query, db_cursor):
    urls = list(search_results.keys())
    descriptions = []
    matched_words = []

    for url in urls:
        db_cursor.execute('SELECT description FROM metadata WHERE url = ?', (url,))
        result = db_cursor.fetchone()
        description = result['description'] if result else ''
        descriptions.append(description)
        matched_words.append(" ".join(search_results[url]))

    tfidf_vectorizer = TfidfVectorizer()
    tfidf_matrix = tfidf_vectorizer.fit_transform(descriptions)

    query_vector = tfidf_vectorizer.transform([query])

    cosine_similarities = linear_kernel(query_vector, tfidf_matrix).flatten()

    ranking_scores = [similarity + len(matched.split()) for similarity, matched in zip(cosine_similarities, matched_words)]

    ranked_results = [(url, score) for url, score in zip(urls, ranking_scores)]
    ranked_results.sort(key=lambda x: x[1], reverse=True)

    formatted_results = []
    for url, score in ranked_results[:10]:
        domain = urlparse(url).netloc
        db_cursor.execute('SELECT title, description FROM metadata WHERE url = ?', (url,))
        result = db_cursor.fetchone()
        search_results[url]=list(set(search_results[url]))
        if result:
            title, description = result
            formatted_results.append({
                'url': url,
                'domain': domain,
                'title': title,
                'description': description,
                'matched_words': search_results[url],
                'ranking_score': score
            })

    return formatted_results

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    query = request.form.get('query', '').lower()
    if query:
        formatted_results = retrieve_search_results(query)
        return render_template('search_results.html', results=formatted_results, query=query)
    else:
        return "Please provide a query parameter 'q' for search."

if __name__ == '__main__':
    app.run(debug=True)
