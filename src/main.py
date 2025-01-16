import json
import os
from datetime import datetime, date
from typing import Dict, List, Set, Any, Generator, DefaultDict, Tuple, Optional
import matplotlib.pyplot as plt
from collections import defaultdict, Counter
import re
import pymorphy3
from nltk.corpus import stopwords
import nltk
import logging
import asyncio
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DateStr = str
MessageData = Dict[str, Any]
TokenList = List[str]
DailyCounts = DefaultDict[date, DefaultDict[str, Dict[str, Any]]]
TopWords = Dict[date, List[Tuple[str, int]]]

class ChatAnalyzer:
    """Handles analysis of chat messages including tokenization and visualization."""

    def __init__(self, data_path: str):
        """
        Initialize the ChatAnalyzer.

        Args:
            data_path: Path to the JSON file containing chat data
        """
        self.data_path = Path(data_path)
        self.morph = pymorphy3.MorphAnalyzer()

        # Ensure NLTK data is available
        try:
            nltk.download('stopwords', quiet=True)
            self.stopwords = set(stopwords.words("russian"))
        except Exception as e:
            logger.error(f"Failed to download NLTK stopwords: {e}")
            self.stopwords = set()

    @staticmethod
    def parse_date(date_str: DateStr) -> datetime:
        """
        Parse date string into datetime object.

        Args:
            date_str: Date string in format "YYYY-MM-DDThh:mm:ss"

        Returns:
            datetime: Parsed datetime object
        """
        try:
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError as e:
            logger.error(f"Failed to parse date {date_str}: {e}")
            raise

    def tokenize_and_lemmatize(self, text: str) -> TokenList:
        """
        Tokenize and lemmatize Russian text.

        Args:
            text: Input text to process

        Returns:
            List of lemmatized tokens
        """
        text = text.lower()
        tokens = re.findall(r'\b\w+\b', text)
        processed_tokens: TokenList = []

        for token in tokens:
            if token in self.stopwords:
                continue
            parsed = self.morph.parse(token)[0]
            if any(pos in parsed.tag for pos in ('NOUN', 'VERB', 'ADJS', 'ADJF')):
                processed_tokens.append(parsed.normal_form)

        return processed_tokens

    async def process_voice_message(self, voice_path: str) -> TokenList:
        """
        Process voice message transcript asynchronously.

        Args:
            voice_path: Path to voice message transcript

        Returns:
            List of processed tokens
        """
        try:
            txt_path = voice_path.replace("voice_messages", "voice_messages_txt").replace(".ogg", ".txt")
            if not os.path.isfile(txt_path):
                return []

            async with asyncio.Lock():  # Prevent concurrent file access
                with open(txt_path, 'r', encoding='utf-8') as file:
                    content = file.read()
            return self.tokenize_and_lemmatize(content)
        except Exception as e:
            logger.error(f"Error processing voice message {voice_path}: {e}")
            return []

    async def analyze_chat(self) -> Tuple[DailyCounts, int]:
        """
        Analyze chat data and generate daily statistics.

        Returns:
            Tuple containing daily counts and number of unrecognized voice messages
        """
        try:
            with open(self.data_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        except Exception as e:
            logger.error(f"Failed to load chat data: {e}")
            raise

        messages = sorted(data["messages"], key=lambda x: self.parse_date(x['date']))
        daily_counts: DailyCounts = defaultdict(
            lambda: defaultdict(lambda: {'messages': 0, 'words': 0, 'tokens': []})
        )
        unrecognized_voice_count = 0

        voice_tasks = []
        for message in messages:
            message_date = self.parse_date(message['date']).date()
            sender = message.get('from', 'Unknown')
            daily_counts[message_date][sender]['messages'] += 1

            if message.get("media_type") == "voice_message":
                task = self.process_voice_message(message['file'])
                voice_tasks.append((message_date, sender, task))
            else:
                text = message.get('text', '')
                if isinstance(text, list):
                    text = ' '.join(
                        item['text'] if isinstance(item, dict) else str(item)
                        for item in text
                    )
                tokens = self.tokenize_and_lemmatize(text)
                daily_counts[message_date][sender]['tokens'].extend(tokens)

        # Process voice messages concurrently
        for message_date, sender, task in voice_tasks:
            tokens = await task
            if tokens:
                daily_counts[message_date][sender]['tokens'].extend(tokens)
            else:
                unrecognized_voice_count += 1

        return daily_counts, unrecognized_voice_count

    def plot_activity(self, daily_counts: DailyCounts, dates: List[date], users: Set[str]) -> None:
        """
        Generate and display activity plot.

        Args:
            daily_counts: Dictionary containing daily statistics
            dates: List of dates to plot
            users: Set of user names
        """
        data_messages = {
            user: [daily_counts[date].get(user, {}).get('messages', 0)
                  for date in dates]
            for user in users
        }

        plt.figure(figsize=(14, 7))
        for user, counts in data_messages.items():
            plt.plot(dates, counts, label=f'{user} - Messages', marker='o')

        date_ticks = dates[::max(1, len(dates) // 10)]
        plt.xticks(date_ticks, [d.strftime('%Y-%m-%d') for d in date_ticks], rotation=45)
        plt.xlabel('Date')
        plt.ylabel('Message Count')
        plt.title('Daily Activity: Messages')
        plt.legend()
        plt.tight_layout()
        plt.show()

async def main() -> None:
    """Main execution function."""
    try:
        analyzer = ChatAnalyzer("result.json")
        daily_counts, unrecognized_count = await analyzer.analyze_chat()

        dates_with_activity = sorted(daily_counts.keys())
        users = {user for date in dates_with_activity for user in daily_counts[date]}

        # Generate word statistics
        top_words_per_day = {}
        for date in dates_with_activity:
            all_tokens: TokenList = []
            for sender_data in daily_counts[date].values():
                all_tokens.extend(sender_data['tokens'])
            word_counter = Counter(all_tokens)
            top_words_per_day[date] = word_counter.most_common(5)

        logger.info(f"Ignored {unrecognized_count} voice messages")
        logger.info("\nTop 5 words per day:")
        for date, top_words in top_words_per_day.items():
            logger.info(f"{date}: {top_words}")

        analyzer.plot_activity(daily_counts, dates_with_activity, users)

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
