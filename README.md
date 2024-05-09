# Big Data Application For Processing Customer Opinions
 
The following dataset is processed from q1.py thru q4.py.  
The way in which the dataset is proccessed are written in comments above the code for the big data application in each of the scripts.

root  
|-- _c0: string (nullable = true)  
|-- Name: string (nullable = true)  
|-- City: string (nullable = true)  
|-- Cuisine Style: string (nullable = true)  
|-- Ranking: string (nullable = true)  
|-- Rating: string (nullable = true)  
|-- Price Range: string (nullable = true)  
|-- Number of Reviews: string (nullable = true)  
|-- Reviews: string (nullable = true)  
|-- URL_TA: string (nullable = true)  
|-- ID_TA: string (nullable = true)  
  
and a preview of the data records looks like the following:  
+---+--------------------+---------+--------------------+-------+------+-----------+-----------------+--------------------+--------------------+----+  
|_c0| Name| City| Cuisine Style|Ranking|Rating|Price Range|Number of Reviews| Reviews| URL_TA| ID_TA|  
+---+--------------------+---------+--------------------+-------+------+-----------+-----------------+--------------------+--------------------+----+  
| 0|Martine of Martin...|Amsterdam|['French', 'Dutch...| 1.0| 5.0| $$ - $$$| 136.0|[['Just like home...|/Restaurant_Revie...|d11752080|  
| 1| De Silveren Spiegel|Amsterdam|['Dutch', 'Europe...| 2.0| 4.5| $$$$| 812.0|[['Great food and...|/Restaurant_Revie...| d693419|  
| 2| La Rive|Amsterdam|['Mediterranean',...| 3.0| 4.5| $$$$| 567.0|[['Satisfaction',...|/Restaurant_Revie...| d696959|  
| 3| Vinkeles|Amsterdam|['French', 'Europ...| 4.0| 5.0| $$$$| 564.0|[['True five star...|/Restaurant_Revie...| d1239229|  
| 4|Librije's Zusje A...|Amsterdam|['Dutch', 'Europe...| 5.0| 4.5| $$$$| 316.0|[['Best meal.... ...|/Restaurant_Revie...| d6864170|  
| 5|Ciel Bleu Restaurant|Amsterdam|['Contemporary', ...| 6.0| 4.5| $$$$| 745.0|[['A treat!', 'Wo...|/Restaurant_Revie...| d696902|  
| 6| Zaza's|Amsterdam|['French', 'Inter...| 7.0| 4.5| $$ - $$$| 1455.0|[['40th Birthday ...|/Restaurant_Revie...| d1014732|  
| 7|Blue Pepper Resta...|Amsterdam|['Asian', 'Indone...| 8.0| 4.5| $$$$| 675.0|[['Great Experien...|/Restaurant_Revie...| d697058|  
| 8|Teppanyaki Restau...|Amsterdam|['Japanese', 'Asi...| 9.0| 4.5| $$$$| 923.0|[['Great Food & S...|/Restaurant_Revie...| d697009|  
| 9|Rob Wigboldus Vis...|Amsterdam|['Dutch', 'Seafoo...| 10.0| 4.5| $| 450.0|[['Excellent Herr...|/Restaurant_Revie...| d1955652|  
| 10| The Happy Bull|Amsterdam|['American', 'Bar...| 11.0| 4.5| $$ - $$$| 295.0|[['Simply AMAZING...|/Restaurant_Revie...|d10275170|  
| 11| Gartine|Amsterdam|['French', 'Dutch...| 12.0| 4.5| $$ - $$$| 967.0|[['A hidden gem',...|/Restaurant_Revie...| d1014753|  
| 12| Restaurant Adam|Amsterdam|['French', 'Europ...| 13.0| 4.5| $$$$| 368.0|[['Love it!', 'As...|/Restaurant_Revie...| d7695005|  
| 13| Biercafe Gollem|Amsterdam| ['Bar', 'Pub']| 14.0| 4.5| $$ - $$$| 586.0|[['Awesome little...|/Restaurant_Revie...| d3893242|  
| 14| Restaurant Daalder|Amsterdam|['French', 'Dutch...| 15.0| 4.5| $$$$| 1246.0|[['Best meal of o...|/Restaurant_Revie...| d1408533|  
| 15|Greenwoods Keizer...|Amsterdam|['Dutch', 'Cafe',...| 16.0| 4.5| $$ - $$$| 1391.0|[['So. Much. Food...|/Restaurant_Revie...| d3200493|  
| 16|Omelegg - City Ce...|Amsterdam|['Dutch', 'Europe...| 17.0| 4.5| $| 1633.0|[['Brunch', 'Wort...|/Restaurant_Revie...| d8562698|  
| 17| Brasserie Ambassade|Amsterdam|['French', 'Bar',...| 18.0| 4.5| $$$$| 958.0|[['Wonderful Chri...|/Restaurant_Revie...| d8567150|  
| 18| Sherpa Restaurant|Amsterdam|['Indian', 'Tibet...| 19.0| 4.5| $$ - $$$| 426.0|[['Very good tibe...|/Restaurant_Revie...| d6022573|  
| 19|La Maschera Lillo...|Amsterdam|['Italian', 'Medi...| 20.0| 4.5| $$ - $$$| 421.0|[['Fabulous Itali...|/Restaurant_Revie...|d10071792|  
+---+--------------------+---------+--------------------+-------+------+-----------+-----------------+--------------------+--------------------+---------+  
only showing top 20 rows  
  
  
  
The following datset is processed in q5.py.   
The way in which the dataset is proccessed are written in comments above the code for the big data application in the script.  
  
root  
|-- movie_id: long (nullable = true)  
|-- title: string (nullable = true)  
|-- cast: string (nullable = true)  
|-- crew: string (nullable = true)  
  
If we take a look at the first rows of the data, we see the following:  
+--------+--------------------+--------------------+--------------------+  
|movie_id| title| cast| crew|  
+--------+--------------------+--------------------+--------------------+  
| 19995| Avatar|[{"cast_id": 242,...|[{"credit_id": "5...|  
| 285|Pirates of the Ca...|[{"cast_id": 4, "...|[{"credit_id": "5...|  
| 206647| Spectre|[{"cast_id": 1, "...|[{"credit_id": "5...|  
| 49026|The Dark Knight R...|[{"cast_id": 2, "...|[{"credit_id": "5...|  
| 49529| John Carter|[{"cast_id": 5, "...|[{"credit_id": "5...|  
| 559| Spider-Man 3|[{"cast_id": 30, ...|[{"credit_id": "5...|  
| 38757| Tangled|[{"cast_id": 34, ...|[{"credit_id": "5...|  
| 99861|Avengers: Age of ...|[{"cast_id": 76, ...|[{"credit_id": "5...|  
| 767|Harry Potter and ...|[{"cast_id": 3, "...|[{"credit_id": "5...|  
  
  
