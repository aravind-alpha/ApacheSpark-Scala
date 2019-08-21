//if else
val age = 43
val offers = if(age>=1 && age<=18)"10% off on sport materials"
else if(age>=19 && age<=40)"40% off on books"
else if(age>=41 && age<=60)"50% off on medicines"
else if(age>61 && age<=99)"80% off on medicines"
else "check your input"

//match
val name = "ram"
val res = name match
{
  case "aravind" => s"Hi $name do development tasks"
  case "sruthi" => s"Hi $name do admin tasks"
  case "siva" => s"Hi $name do network tasks"
  case _ => s"contact the management"
}

//For loop
val x = 1 to 10 toArray
val y = 1 to 5 toArray

for (m<- x) yield m
for (n<- y) yield n
//mutiply 2 arrays using for loop
for (m<-x;n<-y) println(s"$m*$n=${m*n}")

//first 10 sequence of fibonacci series in scala
var a = 0
var b = 0
var i = 0
while (i < 10)
{
  val c = a + b
  a = b
  b = c
  i = i + 1
  if (a < 1) a = 1
  println(b)
}

//Functions
//normal functions - defining a function with a value and calling the function
def sum (x:Int,y:Int) ={x+x * y+y}
sum(3,4)

//anonymous function - a function without a parameter or def=>assigning a expression to a variable
def text = (x:String) => x.toUpperCase().replaceAll("A","*")
def textmsg = "my name is Aravind"
text(textmsg)

//recursive functions - a function calls itself
//factorial
def fact(x:Int):Int = x match
{
  case x if (x<=1)=>1
  case _ => x *fact(x-1)
}
fact(6)
//power
def power(x:Int,n:Int):Int = n match
{
  case n if(n<=0) => 1
  case _ => x*power(x,n-1)
}
power(3,4)

//nested functions - a function which calls within another function
//max
def max(a: Int, b: Int, c: Int) =
{
    def max(x: Int, y: Int) = if (x > y) x else y
     max(a, max(b, c))
}
max(12,131,14)



