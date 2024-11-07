import praw
from prawcore import PrawcoreException
import json
import os
from dotenv import load_dotenv
import pandas as pd
import time
from datetime import datetime, timezone
# from praw.exceptions import RateLimitExceeded, TooManyRequests

# Load environment variables from .env file
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_AGENT = os.getenv("USER_AGENT")
DATA_DIR = os.getenv("DATA_DIR")

SUBREDDIT_NAME = "ArtemisProgram"
MAX_NUM_ROOT_POSTS = 1000 # Same as Reddit's internal limit, so don't think I could set this higher unfortunately
FLUSH_INTERVAL = 5 # Flush local logging buffer to logs.csv every 5 posts
OUTPUT_DIR=os.path.join(DATA_DIR, "data_ArtemisProgram2")

# Initialize logging dataframe
logs_df = pd.DataFrame(columns=['subreddit', 'post_id', 'numComments'])

def initialize_reddit():
    """Initialize and return a reddit instance with our credentials"""
    return praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT
    )
def create_output_directory(directory_name=OUTPUT_DIR):
    """Create the output directory if it doesn't exist."""
    os.makedirs(directory_name, exist_ok=True)
    return directory_name

def fetch_top_posts(subreddit, limit=MAX_NUM_ROOT_POSTS):
    """Fetch the top posts from a given subreddit."""
    # response = subreddit.top(limit=limit) # Use this for ProjectArtemis subreddit where all posts are relevant
    response = subreddit.search("Artemis", sort='relevance', limit=limit) #Use this for nasa and space subreddits
    return response

def extract_post_data(post):
    print(post.id)
    """Extract and return post data along with all comments."""
    post_data = {
        'title': post.title,
        'score': post.score,
        'id': post.id,
        'url': post.url,
        'created_utc': datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),  # Use timezone-aware UTC timestamp
        'author': post.author.name if post.author else "",
        'comments': []
    }
    
    # Load all comments and add them to post_data
    post.comments.replace_more(limit=None)
    for comment in post.comments.list():
        print(f"\t{comment.id}")
        post_data['comments'].append({
            'comment_id': comment.id,
            'comment_body': comment.body,
            'comment_score': comment.score,
            'comment_author': comment.author.name if comment.author else "",
            'comment_timestamp': datetime.fromtimestamp(comment.created_utc, tz=timezone.utc).isoformat(),  # Use timezone-aware UTC timestamp
        })

    post_data['numComments'] = len(post_data['comments']) # Count number of comments to make our lives easier later

    return post_data

def save_post_to_json(post_data, output_dir):
    """Save a single post and its comments to a JSON file in the output directory."""
    file_path = os.path.join(output_dir, f"{post_data['id']}.json")
    with open(file_path, 'w') as file:
        json.dump(post_data, file, indent=4)

def main():

    global logs_df # Ensure we modify the global logs

    # Initialize Reddit and create output directory
    reddit = initialize_reddit()
    output_dir = create_output_directory()
    
    # Access subreddit
    subreddit = reddit.subreddit(SUBREDDIT_NAME)
    
    # Initialize backoff parameters
    backoff_time = 1  # Initial backoff time in seconds
    post_count = 0  # Count of processed posts

    # Fetch top posts and save each to a JSON file
    top_posts = fetch_top_posts(subreddit)
    
    # Get commemnts for all top posts, implementing exponential backoff so as to not hit rate limit
    for post in top_posts:
        output_path = os.path.join(OUTPUT_DIR, f"{post.id}.json")
        if os.path.exists(output_path):
            print(f"{output_path} exists, skipping...")
        else: # If we don't have the data saved, get the comments
            try:
                post_data = extract_post_data(post)
                save_post_to_json(post_data, output_dir)

                # Add entry to DataFrame
                curPost_df = pd.DataFrame([{
                    'subreddit': SUBREDDIT_NAME,
                    'post_id': post_data['id'],
                    'numComments': post_data['numComments']
                }])
                logs_df = pd.concat([logs_df, curPost_df], ignore_index=True)

                post_count += 1

                # Flush logs to CSV periodically
                if post_count % FLUSH_INTERVAL == 0:
                    logs_df.to_csv('logs.csv', mode='a', index=False, header=False)
                    print(f"Data flushed to 'logs.csv' after processing {post_count} posts.")

                backoff_time = 1  # Reset backoff time on success

            except PrawcoreException as e:
                if "429" in str(e): # Too many requests exception (HTTP 429)
                    backoff_time = 2 ** backoff_time  # Exponential backoff
                    print(f"Rate limit exceeded. Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                    continue  # Retry the current post
                else:
                    print(f"An API exception occurred: {e}")
                    break  # Handle other types of API exceptions


    
    # Sum all comments at the end
    total_comments = logs_df['numComments'].sum()
    print(f"Total number of comments across all posts: {total_comments}")
    
    # Save the DataFrame to a CSV file
    logs_df.to_csv('logs.csv', mode='a', index=False, header=False)
    print(f"Data appended to 'logs.csv' in current directory.")

# Usage
if __name__ == "__main__":
    main()