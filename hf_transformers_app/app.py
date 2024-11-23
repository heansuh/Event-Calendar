import re
import torch
from transformers import pipeline
from concurrent.futures import ThreadPoolExecutor
import time
import logging

# Configure logging to display timestamps and debugging information
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Check if MPS (Apple Silicon) is available, otherwise use CPU
device = "mps" if torch.backends.mps.is_available() else "cpu"
logging.info(f"Using device: {'MPS (Apple Silicon)' if device == 0 else 'CPU'}")

# Measure and log how long it takes to load the models
start_time = time.time()
logging.info("Loading NER model...")
ner = pipeline("ner", model="lunesco/bert-german-ner", tokenizer="lunesco/bert-german-ner", 
               aggregation_strategy="simple", device=device)
logging.info(f"NER model loaded. Time taken: {time.time() - start_time:.2f} seconds.")

start_time = time.time()
logging.info("Loading Zero-shot classification model...")
classifier = pipeline(
    "zero-shot-classification", 
    model="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli", 
    tokenizer_kwargs={"clean_up_tokenization_spaces": False}, 
    device=device
)
logging.info(f"Zero-shot classification model loaded. Time taken: {time.time() - start_time:.2f} seconds.")

# Define possible event categories for classification
categories = ["Musik", "Sport", "Theater", "Festival", "Konferenz", "Kunst", "Film"]

# Function to extract entities including Title, Location, Date, Time, and Cost
def extract_entities(text):
    logging.info(f"Extracting entities from text: {text[:50]}...")  # Log the start of entity extraction
    start_time = time.time()
    
    ner_results = ner(text)
    logging.info(f"NER results: {ner_results}")
    
    entities = {
        "Title": "",
        "Location": [],
        "Date": [],
        "Time": [],
        "Cost": []
    }

    # Identify the title based on the first detected named entity
    if ner_results:
        first_entity = ner_results[0]
        if "location" in first_entity["entity_group"].lower() or "organization" in first_entity["entity_group"].lower():
            entities["Title"] = first_entity["word"]

    # Extract other entities
    for entity in ner_results:
        if "location" in entity["entity_group"].lower():
            entities["Location"].append(entity["word"])
        elif "date" in entity["entity_group"].lower():
            entities["Date"].append(entity["word"])
        elif "time" in entity["entity_group"].lower():
            entities["Time"].append(entity["word"])

    # Custom regex for cost detection
    cost_pattern = r"\b\d+(?:[\.,]\d{2})?\s?(?:€|EUR|Euro|Dollar|USD)\b"
    costs = re.findall(cost_pattern, text)
    entities["Cost"].extend(costs)

    logging.info(f"Entities extracted: {entities}. Time taken: {time.time() - start_time:.2f} seconds.")
    return entities

# Function to classify event type using zero-shot classification
def classify_event(text):
    logging.info(f"Classifying event: {text[:50]}...")
    start_time = time.time()
    
    classification = classifier(text, candidate_labels=categories, hypothesis_template="Dies ist ein Beispiel für {}.")
    
    logging.info(f"Classification result: {classification}. Time taken: {time.time() - start_time:.2f} seconds.")
    return classification["labels"][0]  # Return the most probable category

# Function to process each event, combining entity extraction and classification
def process_event(text):
    logging.info(f"Processing event: {text[:50]}...")
    entities = extract_entities(text)
    category = classify_event(text)
    return {"entities": entities, "category": category}

# Sample texts with varying levels of detail
event_texts = [
    "Am 15. Oktober findet im wunderschönen Stadtpark von Hamburg das Herbstfest statt. Von 10:00 Uhr bis 18:00 Uhr erwarten Sie zahlreiche Stände mit regionalen Köstlichkeiten, Kunsthandwerk und Mitmachaktionen für die ganze Familie. Besonders spannend sind die Workshops zur Apfelernte und zum Kürbisschnitzen. Für die kleinen Besucher gibt es ein Karussell und Kinderschminken. Der Eintritt ist frei, aber Spenden sind willkommen. Parkmöglichkeiten sind begrenzt, daher wird die Anreise mit öffentlichen Verkehrsmitteln empfohlen. Kommen Sie vorbei und genießen Sie einen Tag voller Herbstzauber und Unterhaltung!",
    "Das Jazz-Festival Kiel lädt am 22. Oktober zu einem unvergesslichen Abend in die Kulturwerkstatt ein. Ab 19:30 Uhr treten renommierte Musiker aus ganz Deutschland auf. Freuen Sie sich auf mitreißende Klänge und entspannte Atmosphäre. Der Eintritt kostet 20 Euro. Es gibt eine begrenzte Anzahl an Tickets, also sichern Sie sich rechtzeitig Ihren Platz. Snacks und Getränke sind an der Bar erhältlich.",
    "Der Weihnachtsmarkt auf dem Schlossplatz in Stuttgart öffnet am 1. Dezember seine Tore. Täglich von 11:00 bis 21:00 Uhr können Besucher durch festlich geschmückte Buden schlendern und regionale Spezialitäten genießen. Der Eintritt ist frei. Besonders sehenswert sind die handgemachten Weihnachtsdekorationen und das nostalgische Karussell für die Kleinen. Ein Highlight für die ganze Familie!",
    "Die Open-Air-Kino-Nacht in München präsentiert am 17. August ab 20:00 Uhr den Klassiker 'Casablanca' auf der großen Leinwand. Veranstaltungsort ist der Englische Garten, Eintrittskarten kosten 12 Euro. Bitte bringen Sie eigene Decken und Kissen mit. Bei Regen fällt die Veranstaltung aus, aber es gibt einen Ersatztermin. Ein unvergesslicher Abend unter freiem Himmel wartet auf Sie!"
]

# Main application entry point with parallel processing
def main():
    start_time = time.time()
    
    logging.info("Starting event processing...")
    with ThreadPoolExecutor(max_workers=6) as executor:  # Adjust max_workers based on your CPU
        results = executor.map(process_event, event_texts)

    for result in results:
        logging.info(f"Processed Result: Entities: {result['entities']}, Category: {result['category']}")

    logging.info(f"Event processing completed. Total time: {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
