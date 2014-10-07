package boh;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class Main {
	public static void main ( String[] args ) {
		Logger.getRootLogger().setLevel(Level.OFF);
		BasicConfigurator.configure();

		Twitter twitter = new TwitterFactory().getInstance();
		Long lastId = null;

		try {
//			Set<Long> favoriteSet = new HashSet<Long>();
			Set<Long> lastCallSet = new HashSet<Long>();
			Set<Long> thisCallSet = new HashSet<Long>();
			
			while (true) {
				thisCallSet.clear();
				
				Map<String ,RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus();
				for (String endpoint : rateLimitStatus.keySet()) {
					if (endpoint.compareTo("/search/tweets") == 0) {
						RateLimitStatus status = rateLimitStatus.get(endpoint);
						System.err.println(endpoint + " Remaining: " + status.getRemaining());
						System.err.println(endpoint + " SecondsUntilReset: " + status.getSecondsUntilReset());

						if (status.getRemaining() < 10 && status.getSecondsUntilReset() > 0) {
							Thread.sleep(status.getSecondsUntilReset() * 1000);
						}
					}
				}

				Query query = new Query("#datascience OR #rstats OR #bigdata OR #dataviz OR #machinelearning");
				if (lastId != null) {
					query.setSinceId(lastId);	        	
				}

				QueryResult result;
				result = twitter.search(query);
				List<Status> tweets = result.getTweets();
				for (Status tweet : tweets) {
					Status tw = tweet;
					Integer rt = 0;
					if (tweet.isRetweet()) {
						tw = tweet.getRetweetedStatus();
						rt = 1;
					}
					
					thisCallSet.add(tw.getId());

//					if ( !favoriteSet.contains(tw.getId()) ) {
//						System.out.print(
//								tw.getId() + " " +
//								rt + " " +
//								tw.getCreatedAt().getTime() + " "
//								);
						if (rt == 1) {
//							System.out.print("[" + tweet.getUser().getScreenName() + "] ");
						}
//						System.out.println(
//								"@" + tw.getUser().getScreenName() + " " +
//								tw.getText()
//								);
						if (tw.getFavoriteCount() + tw.getRetweetCount() > 0) {
//							favoriteSet.add(tw.getId());
							try {
								twitter.createFavorite(tw.getId());
							}
							catch (TwitterException te) {
								System.out.println("Failed to favorite tweet ("+tw.getId()+"): " + te.getMessage());
							}
						}
//					}
				}
				
				Integer observed = thisCallSet.size();
				Set<Long> tcs = thisCallSet;
				tcs.retainAll(lastCallSet);
				Integer duplicate = thisCallSet.size();
				
				Float s = 1F * duplicate/observed;
				System.err.println("sleeping " + s + " minutes");
				Thread.sleep(60000 * duplicate/observed);
				
				lastCallSet = thisCallSet;
			}
		}
		catch (TwitterException te) {
//			te.printStackTrace();
			System.out.println("Failed to search tweets: " + te.getMessage());
			System.exit(-1);
		}
		catch (InterruptedException ie) {
			System.exit(-1);
		}
	}
}
