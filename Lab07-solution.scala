val textFile = sc.textFile("10mb.txt")
textFile.cache()
textFile.count() //short processing time will be incurred
textFile.count() //almost instantaneous result

