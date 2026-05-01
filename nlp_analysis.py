import duckdb
import pandas as pd
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import Counter
import re

# ---- config ----
DB_PATH = "./olist_analytics/olist.duckdb"
MIN_REVIEWS = 5  # minimum term frequency to include

# ---- load data ----
print("→ Loading 1-star reviews...")
con = duckdb.connect(DB_PATH)

reviews = con.execute("""
    select
        order_id,
        review_score,
        review_text,
        review_title
    from main.fct_customer_orders
    where review_score = 1
        and review_text is not null
        and trim(review_text) != ''
""").df()

print(f"  Found {len(reviews)} reviews")

# ---- preprocess ----
print("→ Preprocessing text with spaCy...")
nlp = spacy.load("pt_core_news_sm")

def preprocess(text):
    if not text or not isinstance(text, str):
        return ""
    # lowercase and remove special characters
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    doc = nlp(text)
    # keep only meaningful tokens
    tokens = [
        token.lemma_
        for token in doc
        if not token.is_stop
        and not token.is_punct
        and not token.is_space
        and len(token.lemma_) > 2
    ]
    return " ".join(tokens)

reviews["processed_text"] = reviews["review_text"].apply(preprocess)
reviews = reviews[reviews["processed_text"].str.strip() != ""]

print(f"  {len(reviews)} reviews after preprocessing")

# ---- TF-IDF ----
print("→ Running TF-IDF...")
vectorizer = TfidfVectorizer(
    max_features=100,
    ngram_range=(1, 2),  # single words and bigrams
    min_df=MIN_REVIEWS
)

tfidf_matrix = vectorizer.fit_transform(reviews["processed_text"])
feature_names = vectorizer.get_feature_names_out()

# average TF-IDF score per term across all reviews
mean_scores = tfidf_matrix.mean(axis=0).A1
term_scores = sorted(
    zip(feature_names, mean_scores),
    key=lambda x: x[1],
    reverse=True
)

# ---- build results dataframe ----
results = pd.DataFrame(term_scores, columns=["term", "tfidf_score"])
results["rank"] = range(1, len(results) + 1)

# also get raw frequency
all_tokens = " ".join(reviews["processed_text"]).split()
freq = Counter(all_tokens)
results["frequency"] = results["term"].map(lambda t: freq.get(t, 0))

print(f"  Top 10 terms:")
print(results.head(10).to_string(index=False))

# ---- save to duckdb ----
print("→ Saving results to DuckDB...")
con.execute("drop table if exists main.review_term_frequencies")
con.execute("""
    create table main.review_term_frequencies as
    select * from results
""")

# also save the processed reviews themselves for further analysis
con.execute("drop table if exists main.reviews_processed")
con.execute("""
    create table main.reviews_processed as
    select
        order_id,
        review_score,
        review_text,
        processed_text
    from reviews
""")

con.close()
print("✓ Done!")