## Simple reactive web application that gets yoda quotes from ordinary web application.

<a name="build-project"></a>
### Build project
```
mvn clean package
```
------------------------

### Testing
In order to test the application use this requests:

To get Mono quote from web application:
```
GET http://localhost:8090/api/quote
```

To get Mono error response from web application and just log that error:
```
GET http://localhost:8090/api/quote/error
```

To get Mono quote from web application with delay and duration of the request is logged:
```
GET http://localhost:8090/api/quote/delay
```

To get Subscribe to the quotes updates:
```
GET http://localhost:8090/api/quotes
```

To publish new quote:
```
GET http://localhost:8090/api/quote
```


