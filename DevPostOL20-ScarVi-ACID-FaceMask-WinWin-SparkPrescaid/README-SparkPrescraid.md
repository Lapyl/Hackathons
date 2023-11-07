# Spark PrescrAid

Covid-19 pandemic has warned us that the regulatory system for medicines should not stop at the approval and release. Apache Spark can help us improve this system.

This project proposes a system, in which medicines’ reactions are reported, and a Spark based artificial intelligence tool aids medical doctors in prescribing the best medicines for a given set of ailments.

We propose web sites, where people can report reactions of various medicines in the presence of various health conditions.

Reports will be accumulated on a server, which will feed the data into a Spark based machine learning algorithm. The streaming data will develop and update models for medicine appropriateness.

For frontend part of the project, we have set up an illustrative web site at lipy.us/sparkai and preaid.net/sparkai.

The Map page displays a choropleth map of Covid-19 cases, based on mapael and Raphael jQueries. The page will show markers of recent reports and enquiries.

The Report page contains a form for reporting illnesses before medication, medicines used, illnesses cured, and illnesses acquired. If the location is entered, the Map page will mark it.

The Enquire page contains a form for enquiring for medicines appropriate for existing illnesses. If the location is entered, the Map page will mark it.

The Simulate page allows an administrator to generate and stream data for building and testing a model.

The Illnesses and Medicine pages give lists of common illnesses and medicines, along with links to useful articles.

For backend part of the project, we have started with a few experiments. We created and streamed dummy files of records of one illness and one medicine and its effectiveness.

As expected we found that Spark is capable of handling big files that cannot be handled by Python or R without Spark. Spark Streaming is very fast in processing streams of files or data.

Unless we use Kafka or something, Spark Streaming may halt and even stop if there are not enough streams. This requires continuous streaming of files or data if we want to use Spark Streaming.
