package com.unm.app.nyt;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.unm.app.nyt.model.NewsItem;
import com.unm.app.nyt.model.NewsItemReply;

import com.unm.app.nyt.constants.Constants;

public class NewsArticlesApp {

	private static final Logger LOGGER = LogManager
			.getLogger(NewsArticlesApp.class);

	private static final String DATETIME = (new SimpleDateFormat(
			"yyyy-MM-dd_HH-mm-ss")).format(Calendar.getInstance().getTime());

	private static final int TOTAL_NEWS_ITEMS_TO_BE_EXTRACTED = 50000;
	private static final String OUTPUT_ABSTRACT_JSON = "output/abstract"
			+ DATETIME + ".json";

	private static int totalNumberofNewsItemsNeeded = TOTAL_NEWS_ITEMS_TO_BE_EXTRACTED;
	private static int startingOffset = 0;
	private static final List<NewsItem> totalNewsItems = new ArrayList<NewsItem>(
			totalNumberofNewsItemsNeeded);
	private static List<NewsItem> dupNewsItems = new ArrayList<NewsItem>();
	private static final Set<String> urlSet = new HashSet<String>();

	public static void main(String[] args) {
		LOGGER.info("Started Collecting news items");
		try {
			process();
			LOGGER.info("Successfully collected news items");
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("Error in collecting news items " + e);
		}
	}

	private static void process() throws FileNotFoundException, IOException,
			InterruptedException {
		OutputStream abstractOutput;
		final RestTemplate restTemplate = new RestTemplate();
		while (totalNumberofNewsItemsNeeded > 0) {
			abstractOutput = initFileOutput(OUTPUT_ABSTRACT_JSON);
			try {
				NewsItemReply reply = callUrlForNewsItem(restTemplate,
						startingOffset);
				collectReply(reply, abstractOutput);
			} catch (RestClientException e) {
				LOGGER.error(e.getMessage() + ", Retrying....");
				continue;
			}
			startingOffset += 20;
			totalNumberofNewsItemsNeeded -= 20;
			Thread.sleep(1000);
		}
		/*while (totalNewsItems.size() < TOTAL_NEWS_ITEMS_TO_BE_EXTRACTED) {
			int numberOfDuplicateItems = dupNewsItems.size();
			if (numberOfDuplicateItems != 0) {
				dupNewsItems = new ArrayList<NewsItem>();
				for (int offset = TOTAL_NEWS_ITEMS_TO_BE_EXTRACTED; offset < TOTAL_NEWS_ITEMS_TO_BE_EXTRACTED
						+ numberOfDuplicateItems; offset++) {
					NewsItemReply reply = callUrlForNewsItem(restTemplate,
							offset);
					collectReply(reply);
				}
			} else {
				break;
			}
		}*/
//		writeNewsItemsToFile(abstractOutput, totalNewsItems);
//		closeFileOutput(abstractOutput);
	}

	private static NewsItemReply callUrlForNewsItem(
			final RestTemplate restTemplate, int offset) throws IOException {
		String urlForRetrievingNewsItems = "http://api.nytimes.com/svc/news/v3/content/all/all/.json?offset="
				+ offset
//				+ "&api-key=28f01a9d4259d61015c41f011342d35a:13:69904887";
				+ "&api-key=5b153a6d106bd21d308fd12d3e0fd417:15:69898101";
		LOGGER.info("Collecting news items with offset " + offset
				+ ", total news items to be collected "
				+ totalNumberofNewsItemsNeeded);
		try {
			NewsItemReply reply = restTemplate.getForObject(
					urlForRetrievingNewsItems, NewsItemReply.class);
			return reply;
		} catch (Exception e) {
			throw new RestClientException("Error collecting news item for url "
					+ urlForRetrievingNewsItems);
		}
	}

	private static void writeNewsItemsToFile(final OutputStream output,
			List<NewsItem> totalNewsItems) throws IOException {
		LOGGER.trace("Started writing news items to file "
				+ ((FileOutputStream) output).getFD());
		int count = 0;
		for (NewsItem newsItem : totalNewsItems) {
			count += 1;
			StringBuffer buffer = new StringBuffer();
			buffer.append(count).append(Constants.PARAM_SEPARATOR)
					.append(newsItem.getUrl())
					.append(Constants.PARAM_SEPARATOR)
					.append(newsItem.getAbstractForNews());
			output.write((buffer.toString() + "\n").getBytes());
			output.flush();
		}
		LOGGER.trace("Completed writing news items to file");
	}

	private static void collectReply(final NewsItemReply reply, OutputStream abstractOutput)
			throws IOException {
		if ("OK".equals(reply.getStatus())) {
			List<NewsItem> results = reply.getResults();
			for (NewsItem newsItem : results) {
				boolean isAdded = urlSet.add(newsItem.getUrl());
				if (isAdded) {
					totalNewsItems.add(newsItem);
				} else {
					dupNewsItems.add(newsItem);
				}
			}
			writeNewsItemsToFile(abstractOutput, totalNewsItems);
			closeFileOutput(abstractOutput);
		} else {
			LOGGER.info("Reply status is not ok for the offset"
					+ startingOffset);
		}
	}

	private static OutputStream initFileOutput(final String path)
			throws FileNotFoundException {
		LOGGER.trace("Intializing file output " + path + " in replace mode");
		FileOutputStream outputStream = new FileOutputStream(path, true);
		LOGGER.trace("Done intializing file output " + path
				+ " in replace mode");
		return outputStream;
	}

	private static void closeFileOutput(final OutputStream output)
			throws IOException {
		if (output != null) {
			output.flush();
			output.close();
		}
	}
}
