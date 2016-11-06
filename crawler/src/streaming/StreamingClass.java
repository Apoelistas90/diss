package streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import db.MySqlConn;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import util.Stopwatch;

public class StreamingClass {
	
	
	 public static void main(String[] args) throws TwitterException {
		
		Stopwatch time = new Stopwatch();
		
		
		 
	//	final String db = "footballdb"; //First
		final String db = "secondfootballdb"; //Second
		String user ="Apoelistas9000";//Second
		//String user = "AndysAws"; //First
		String psw = "andrikkos7"; 
		//String hostname = "footballdbinstance.c5cfl5n7kjng.eu-west-1.rds.amazonaws.com";//First
		String hostname = "footballdbinstance.c1ct7lfma7ye.eu-west-1.rds.amazonaws.com";//Second
		String port = "3306";
		
		final String teams[] = new String[20];
		teams[0] = "arsenal";
		teams[1] = "astonvilla";
		teams[2] = "cardiff";
		teams[3] = "chelsea";
		teams[4] = "crystalpalace";
		teams[5] = "everton";
		teams[6] = "fulham";
		teams[7] = "hull";
		teams[8] = "liverpool";
		teams[9] = "mcity";
		teams[10] = "munited";
		teams[11] = "newcastle";
		teams[12] = "norwich";
		teams[13] = "southampton";
		teams[14] = "stoke";
		teams[15] = "sunderland";
		teams[16] = "swansea";
		teams[17] = "tottenham";
		teams[18] = "westbrom";
		teams[19] = "westham";
		
		final MySqlConn conn = new MySqlConn(db, user, psw, hostname, port);
		
		 createDatabaseTables(conn,db,teams);
		 
		 ConfigurationBuilder cb = new ConfigurationBuilder();
		 cb.setDebugEnabled(true);
		 cb.setOAuthConsumerKey("L0LJ9GQb1Q2UdIPY96w3yw");
		 cb.setOAuthConsumerSecret("r2s6hS709x7gNaIOHzO7mOxaECuRIhapDU0zNo84UTg");
		 cb.setOAuthAccessToken("199872881-cuNNuDTolVYNlVF4Cv49Bhm4EqC63dXD8S3E8IWD");
		 cb.setOAuthAccessTokenSecret("0pkRLD9Dg5qPlpAT7PufdGcCQUJyIChbRmLmooNX6BRzk");
		 
		 TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		 
		 String arsenal[] = {"#arsenal","#arsenalfc", "#afc", "#gunners", "#thegunners","#gooners","#comeongunners", "#comeongooners" ,"#comeonyougunners", "#coyg", "#comeonarsenal","#arsenalfamily", "#afcfamily"};
		 String astonvilla[] = { "#astonvillafc", "#villans", "#avfc", "#astonvilla", "#govilla", "#comeonastonvilla", "#comeonvilla", "#comeonyouvillans", "#villafamily", "#avfcfamily"};
		 String cardiff[] = { "#cardiff","#cardiffcity" , "#ccfc" , "#comeoncardiff" , "#cardifffc","#cardiffcityfamily","#cccfcfamily","#comeoncardiff","#comeonyoucardiff"};
		 String chelsea[] = {"#chelsea","#chelseafc","#cfc","#ktbffh", "#gochelsea", "#cfcfamily", "#letsgochelsea", "#comeonchelsea" , "#comeoncfc", "#cfcfamily", "#chelseafcfamily","#cfcfamily"};
		 String crystalpalace[] = {"#crystalpalace","#crystalpalacefc" , "#cpfc","#crystalpalacefamily","#cpfcfamily","#comeoncrystalpalace"};
		 String everton[] = {"#everton", "#evertonfc", "#efc", "#evertonfamily", "#schoolofscience", "#toffees", "#toffeemen","#coyb", "#comeoneverton","#comeontoffees", "#goeverton","#efcfamily"};
		 String fulham[] = { "#fulham","#fulhamfc", "#cottagers", "#ffc", "#gofulham","#fulhamfamily", "#comeonfulham", "#comeonyouwhites", "#coyws","#ffcfamily"};
		 String hull[] = {"#hull","#hullfc", "#hullcityfc","#hcafc", "#hullcity", "#hcfc", "#comeonhull", "#hullcityfamily","#comeonyouhull"};
		 String liverpool[] = {"#liverpool", "#liverpoolfc","#lfc","#goliverpool","#youwillneverwalkalone","#ynwa","#neverwalkalone","#comeonlfc", "#comeonliverpool", "#redordead", "#lfcfamily"};
		 String mcity[] = {"#manchestercity", "#manchestercity", "#manchestercityfc","#mancity", "#mancityfc" ,"#citizens" ,"#mcfc","#gomcfc" ,"#gomancity" ,"#comeonmancity","#comeoncitizens","#mancityfamily","#mcfcfamily"};
		 String munited[] = {"#manchesterunited", "#manchesterunitedfc", "#manutd", "#manunited", "#manunitedfc", "#reddevils" ,"#mufc", "#gomanutd", "#gomanunited", "#foreverunited" ,"#gomufc", "#gloryglorymanunited", "#ggmu","#manutdfamily","#mcfcfamily"};
		 String newcastle[] = {"#newcastle","#newcastleunited","#nufc", "newcastlefc", "#nufcroundup","#comeonnewcastle","#toonarmy","#nufcfamily"};
		 String norwich[] = {"#norwichfc", "#norwichcity", "#norwichcityfc", "#comeonyoucanaries", "#ncfc", "#comeonnorwich","#ncfcfamily"};
		 String southampton[] = {"#southampton","#southamptonfc", "#saints", "#saintsfc", "#upthesaints", "#comeonsouthampton","southamptonfcfamily"};
		 String stoke[] = {"#stoke","#stokecity", "#stokecityfc", "#scfc", "#comeonstoke", "#comeonyoustoke", "#comeonpotters", "#comeonyoupotters","#scfcfamily"};
		 String sunderland[] = {"#sunderland", "#sunderlandfc", "#safc", "#mackems", "#comeonblackcats", "#comeonsunderland", "#comeonyoumackems", "#comeonyousunderland", "#comeonyoublackcats", "#gosunderland", "#goblackcats","#safcfamily"};
		 String swansea[] = {"#swansea","#swanseacity","#swanseacityfc","#swanseafc", "#swans", "#swansfc", "#jackarmy", "#comeonswansea", "#twitterjacks","#comeonyoujacks"};
		 String tottenham[] = {"#tottenham", "#tottenhamfc", "#tottenhamhotspur", "#tottenhamhotspurfc", "#yids", "#yidarmy", "#comeonyouspurs", "#coys", "#thfc", "#hotspurs", "#spursfamily","#thfcfamily"};
		 String westbrom[] = {"#westbromwich","#westbromwichalbionfc", "#westbromwichfc", "#westbromfc", "#westbrom", "#baggies", "#wba", "#wbafc", "#wbfc", "#comeonbaggies", "#comeonyoubaggies", "#gowba", "#gobaggies"};
		 String westham[] = {"#westham","#westhamunited", "#westhamunitedfc", "#whu", "#whufc","#westhamfamily", "#whtid", "#comeonwestham", "#comeonyouirons", "#coyi", "#comeonwestham", "#gowestham","#whufcfamily"};
		 
		 String all[] = {"#arsenal","#arsenalfc", "#afc", "#gunners", "#thegunners","#gooners","#comeongunners", "#comeongooners" ,"#comeonyougunners", "#coyg", "#comeonarsenal","#arsenalfamily", "#afcfamily",
				 "#astonvillafc", "#villans", "#avfc", "#astonvilla", "#govilla", "#comeonastonvilla", "#comeonvilla", "#comeonyouvillans", "#villafamily", "#avfcfamily",
				 "#cardiff","#cardiffcity" , "#ccfc" , "#comeoncardiff" , "#cardifffc","#cardiffcityfamily","#cccfcfamily","#comeoncardiff","#comeonyoucardiff",
				 "#chelsea","#chelseafc","#cfc","#ktbffh", "#gochelsea", "#cfcfamily", "#letsgochelsea", "#comeonchelsea" , "#comeoncfc", "#cfcfamily", "#chelseafcfamily","#cfcfamily",
				 "#crystalpalace","#crystalpalacefc" , "#cpfc","#crystalpalacefamily","#cpfcfamily","#comeoncrystalpalace",
				 "#everton", "#evertonfc", "#efc", "#evertonfamily", "#schoolofscience", "#toffees", "#toffeemen","#coyb", "#comeoneverton","#comeontoffees", "#goeverton","#efcfamily",
				 "#fulham","#fulhamfc", "#cottagers", "#ffc", "#gofulham","#fulhamfamily", "#comeonfulham", "#comeonyouwhites", "#coyws","#ffcfamily",
				 "#hull","#hullfc", "#hullcityfc","#hcafc", "#hullcity", "#hcfc", "#comeonhull", "#hullcityfamily","#comeonyouhull",
				 "#liverpool", "#liverpoolfc","#lfc","#goliverpool","#youwillneverwalkalone","#ynwa","#neverwalkalone","#comeonlfc", "#comeonliverpool", "#redordead", "#lfcfamily",
				 "#manchestercity", "#manchestercity", "#manchestercityfc","#mancity", "#mancityfc" ,"#citizens" ,"#mcfc","#gomcfc" ,"#gomancity" ,"#comeonmancity","#comeoncitizens","#mancityfamily","#mcfcfamily",
				 "#manchesterunited", "#manchesterunitedfc", "#manutd", "#manunited", "#manunitedfc", "#reddevils" ,"#mufc", "#gomanutd", "#gomanunited", "#foreverunited" ,"#gomufc", "#gloryglorymanunited", "#ggmu","#manutdfamily","#mcfcfamily",
				 "#newcastle","#newcastleunited","#nufc", "newcastlefc", "#nufcroundup","#comeonnewcastle","#toonarmy","#nufcfamily",
				 "#norwichfc", "#norwichcity", "#norwichcityfc", "#comeonyoucanaries", "#ncfc", "#comeonnorwich","#ncfcfamily",
				 "#southampton","#southamptonfc", "#saints", "#saintsfc", "#upthesaints", "#comeonsouthampton","southamptonfcfamily",
				 "#stoke","#stokecity", "#stokecityfc", "#scfc", "#comeonstoke", "#comeonyoustoke", "#comeonpotters", "#comeonyoupotters","#scfcfamily",
				 "#sunderland", "#sunderlandfc", "#safc", "#mackems", "#comeonblackcats", "#comeonsunderland", "#comeonyoumackems", "#comeonyousunderland", "#comeonyoublackcats", "#gosunderland", "#goblackcats","#safcfamily",
				 "#swansea","#swanseacity","#swanseacityfc","#swanseafc", "#swans", "#swansfc", "#jackarmy", "#comeonswansea", "#twitterjacks","#comeonyoujacks",
				 "#tottenham", "#tottenhamfc", "#tottenhamhotspur", "#tottenhamhotspurfc", "#yids", "#yidarmy", "#comeonyouspurs", "#coys", "#thfc", "#hotspurs", "#spursfamily","#thfcfamily",
				 "#westbromwich","#westbromwichalbionfc", "#westbromwichfc", "#westbromfc", "#westbrom", "#baggies", "#wba", "#wbafc", "#wbfc", "#comeonbaggies", "#comeonyoubaggies", "#gowba", "#gobaggies",
				 "#westham","#westhamunited", "#westhamunitedfc", "#whu", "#whufc","#westhamfamily", "#whtid", "#comeonwestham", "#comeonyouirons", "#coyi", "#comeonwestham", "#gowestham","#whufcfamily"};
		 
		 final List<String[]> hashtags = new ArrayList<String[]>();
		 hashtags.add(arsenal);hashtags.add(astonvilla); hashtags.add(cardiff);hashtags.add(chelsea);
		 hashtags.add(crystalpalace);hashtags.add(everton); hashtags.add(fulham);hashtags.add(hull);
		 hashtags.add(liverpool);hashtags.add(mcity); hashtags.add(munited);hashtags.add(newcastle);
		 hashtags.add(norwich);hashtags.add(southampton); hashtags.add(stoke);hashtags.add(sunderland);
		 hashtags.add(swansea);hashtags.add(tottenham); hashtags.add(westbrom);hashtags.add(westham);
		 
		 final int totalTweets = 0;
		 
		 StatusListener listener = new StatusListener() {
			
			 int totalTweets = 0;
			 String createdAt,userScreenName,tweet,coord = "";
			int userFollowers,retweetCount,favoriteCount = 0;
			
			String regex = "\\'";
			String replacement = "";
			
			String retweet="";
			String queryCheck="";
			String updateQuery="";
			String query="";
			int checker=0;
			
			@Override
			public void onException(Exception arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onStatus(Status status) {
				
				
				
				//System.out.println("status.getIsoLanguageCode().equals('en') : "+status.getIsoLanguageCode().equals("en"));
				
				if (status.getIsoLanguageCode().equals("en")){
				    if(true){
					//if (!status.isRetweet()){
				    	totalTweets++;
				    	
						createdAt  = status.getCreatedAt().toString();						
						userScreenName = status.getUser().getScreenName().toString();	
						tweet = status.getText().toString();
						if(String.valueOf(totalTweets).matches("([1][0]*|[5][0]*)")){
				    		System.out.println(createdAt + "  :  "+totalTweets +" tweets retieved so far!");
				    		System.out.println("The " +totalTweets+"th retrieved tweet is: " + tweet);
				    	};
						if(status.getGeoLocation()!=null){
							double lat = status.getGeoLocation().getLatitude();
							double lon = status.getGeoLocation().getLongitude();
							coord = lat+","+lon;
						}else{
							coord = "null";
						}
						
						userFollowers = status.getUser().getFollowersCount();											
						retweetCount = status.getRetweetCount();
						favoriteCount = status.getFavoriteCount();
						
						//System.out.println("Tweet:" + tweet);
						//need to remove apostrophes
						
						tweet = tweet.replaceAll(regex,replacement);
						
						
						//check if tweet is here
						int i = 0;
						outer:
						for(String[] array : hashtags){							
							for(String hashtag:array){							
								if(StringUtils.containsIgnoreCase(tweet, hashtag)){
								//if (tweet.contains(hashtag)){
									
									if(tweet.startsWith("RT")){
										//System.out.println("Found a retweet for: "+ teams[i]);
										//Get tweet
										retweet = status.getRetweetedStatus().getText();
										//remove apostrophes for retweet
										retweet = retweet.replaceAll(regex,replacement);
										
										queryCheck = "SELECT count(*) from `" + db + "_"+ teams[i] + "` WHERE tweet = ?";
										checker = conn.executePreparedQuery( queryCheck,retweet );
									
										//if yes -> increment retweet count of tweet without the prefix RT
										if(checker>0){
											// System.out.println("Inside UPDATES, incrementing retweet count");
											 //update query
											 updateQuery = "UPDATE `" + db + "_"+ teams[i] + "` ";
											 updateQuery+= "SET retweeted=retweeted+1 ";
											 updateQuery+= "WHERE tweet = '"+retweet+"';";
											// System.err.println(updateQuery );
											 conn.executeQuery( updateQuery );
											
											 break outer;
										}else{
											break outer;
										}										
									}
									//Not a retweet
									else{
										//add new tweet to its table
										//System.out.println("Found a new tweet for: "+ teams[i]);
										query  = "INSERT INTO `" + db + "_"+ teams[i] + "` (createdat,screenName,tweet,coord,followers,retweeted,favorited) VALUES ";
										query += " ("; 
										//DateTime
										query += "'" + createdAt + "',"; 
										query += "'" + userScreenName + "',";
										query += "'" + tweet + "',";
										query += "'" + coord + "',";
										query += userFollowers + ",";
										query += retweetCount + ",";
										query += favoriteCount + "";
										query += ");";
										//System.err.println( query );
										conn.executeQuery( query );
										System.gc();
										break outer;
									}																
					
								}
								
							}
							i++;
						}
						
						
						
					}
				}				
			}

			@Override
			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub
				
			}
			 
		 };
		 
		 FilterQuery fq = new FilterQuery();
		 
		 fq.track(all);
		 
		 twitterStream.addListener(listener);
		 System.out.println("Listener added!");
		 twitterStream.filter(fq); 
		 System.out.println("Filters set!");		 
		 all = null;
	 }


	//@SuppressWarnings("unused")
	private static void createDatabaseTables(MySqlConn conn,String db, String[] teams) {
		 
		 for(String s : teams){
			 String table = db + "_" + s;
			 //conn.executeQuery( "use " + db );	
			 String query;
			 query  = "CREATE TABLE `" + table + "` (";
			 query += "  id int(11) NOT NULL AUTO_INCREMENT,";
			 query += "  createdat varchar(255),";
			 query += "  screenName varchar(255),";
			 query += "  tweet varchar(255),";
			 query += "  coord varchar(255),";
			 query += "  followers int(11) NOT NULL,";
			 query += "  retweeted int(11) NOT NULL,";
			 query += "  favorited int(11) NOT NULL,";
			 query += "  PRIMARY KEY (id)";
			 query += ") ENGINE=InnoDB DEFAULT CHARSET=latin1;";
			// System.out.println("query: " + query);
			 conn.executeQuery(query);		
		 }
				
	}
}
