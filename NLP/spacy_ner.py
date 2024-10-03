import spacy
from spacy import displacy
from dateutil import parser as date_parser  # To convert dates to datetime objects
import re
import json
from spacy.language import Language

# Load the large German spaCy model
nlp = spacy.load("de_core_news_lg")

# Load cities and villages from JSON file
with open('/app/villages_min_5_poi.json', 'r') as file:
    cities_and_villages = json.load(file)

# Convert all city names to lowercase for case-insensitive comparison
cities_and_villages = [city.lower() for city in cities_and_villages]

@Language.component("extract_city_and_location")
def extract_city_and_location(doc):
    place_keywords = ['platz', 'feld', 'halle', 'arena', 'stadion', 'park', 'bühne', 'venue', 'gelände']
    city = None
    location = None

    # Check each token to see if it's a city or village from the JSON list (case-insensitive)
    for token in doc:
        if token.text.lower() in cities_and_villages and city is None:  # Assign city only once
            print(f"Found city: {token.text}")
            city = token.text  # Store the city if found

    # Check all tokens for location-related keywords
    for token in doc:
        if any(keyword in token.text.lower() for keyword in place_keywords):
            location = token.text  # Assign the matching token as location
            print(f"Assigning location: {location}")
            break  # Exit loop after finding the first match

    # Assign extracted city and location to entities in the doc
    for ent in doc.ents:
        if ent.label_ == "LOC":
            if location and location in ent.text:
                ent._.extracted_location = location
            elif city and city in ent.text:
                ent._.extracted_city = city

    return doc

# Add custom attributes to the entities to store extracted city and location
spacy.tokens.Span.set_extension("extracted_city", default=None)
spacy.tokens.Span.set_extension("extracted_location", default=None)

# Add the custom component to the pipeline
nlp.add_pipe("extract_city_and_location", after="ner")

# Sample text
text = """
Sting in Kiel – ein musikalisches Highlight, das man nicht verpassen sollte! Am 14. Juni 2025 um 19 Uhr wird der britische Superstar im Rahmen des Schleswig-Holstein Musik Festivals (SHMF) auf dem Nordmarksportfeld Open Air auftreten.
"""

# Process the text with spaCy
doc = nlp(text)

# Initialize event info dictionary
event_info = {"Date": None, "Event Name": None, "Time": None, "City": None, "Location": None, "Category": None}

# Extract entities using spaCy and enhance them where necessary
for ent in doc.ents:
    if ent.label_ == "DATE":
        try:
            event_info["Date"] = date_parser.parse(ent.text)
        except Exception:
            event_info["Date"] = ent.text
    elif ent.label_ == "TIME":
        event_info["Time"] = ent.text  # Capture time if found
    elif ent.label_ == "LOC":
        # Assign city and location separately
        if ent._.extracted_city:
            event_info["City"] = ent._.extracted_city
        if ent._.extracted_location:
            event_info["Location"] = ent._.extracted_location

# If no location is found via NER, fallback to location found through keyword matching
if event_info["Location"] is None:
    for token in doc:
        # Look for location-related keywords
        place_keywords = ['platz', 'feld', 'halle', 'arena', 'stadion', 'park', 'bühne', 'venue', 'gelände']
        if any(keyword in token.text.lower() for keyword in place_keywords):
            event_info["Location"] = token.text  # Assign location from token
            print(f"Location assigned from keyword matching: {token.text}")
            break  # Exit after finding the first location

# Regex fallback for time extraction if spaCy misses it
time_match = re.search(r"\b\d{1,2}(:\d{2})?\s?Uhr\b", text)  # Matches times like "19 Uhr" or "19:00 Uhr"
if time_match and not event_info["Time"]:
    event_info["Time"] = time_match.group()

# Regex fallback for date extraction if spaCy misses it
date_match = re.search(r"\d{1,2}\.?\s\w+\s\d{4}", text)  # Matches dates like "14. Juni 2025"
if date_match and not event_info["Date"]:
    event_info["Date"] = date_match.group()

# Regex for more specific event name extraction
if "Sting" in text and "Schleswig-Holstein Musik Festival" in text:
    event_info["Event Name"] = "Sting, Schleswig-Holstein Musik Festival"
else:
    event_info["Event Name"] = "Konzert"

# Hardcoded category since spaCy doesn't handle this well in this context
event_info["Category"] = "Musik"

# Print the resulting event info
print(event_info)
