public class AreaCalculator {
    public static double area(double radius)
    {
        if( radius < 0 )
        {
            return -1.0d;
        }
        return Math.pow(radius, 2) * Math.PI;
    }

    public static double area(double x, double y)
    {
        if( x < 0 || y < 0)
        {
            return -1;
        }
        return x*y;
    }
}

public class Main {
    public static  void  main(String[] arg){
        System.out.println(AreaCalculator.area(5.0));
        System.out.println(AreaCalculator.area(-1));
        System.out.println(AreaCalculator.area(5.0, 4.0));
        System.out.println(AreaCalculator.area(-1.0, 4.0));
        System.out.println(AreaCalculator.area(9.0));
    }
}
####out#####
78.53981633974483
-1.0
20.0
-1.0
254.46900494077323
#####################################################
//The method needs to return an area of a rectangle.

public class BarkingDog {
//    public static boolean shouldWakeUp (boolean barking, int hourOfDay) {
//        if (!barking) return false;
//        return(hourOfDay >= 0 && hourOfDay < 8)  || hourOfDay == 23 ? true : false ;
//    with printing ...
    public static boolean shouldWakeUp(boolean barking,int hourOfDay){
        if(barking == true){
            if(hourOfDay >= 0 && hourOfDay < 8){
                System.out.println("true");
                return true;
            }
            else if(hourOfDay > 22 && hourOfDay <= 23){
                System.out.println("true");
                return true;
            }
            else {
                System.out.println("true");
                return true;
            }

        }
        else{
            System.out.println("false");
            return false;
        }
    }
}

public class Main {
    public static  void  main(String[] arg) {
        BarkingDog.shouldWakeUp(true, 1);
        BarkingDog.shouldWakeUp(false, 2);
        BarkingDog.shouldWakeUp(true, 8);
        BarkingDog.shouldWakeUp(true, 23);
        BarkingDog.shouldWakeUp(true, -1);
    }
}
##########out##############
true
false
true
true
true
#############################
package com.larisa;

public class Main {

    // int has a width of 32
    public static void main(String[] args) {
	int myMinValue=-2_147_483_648;
	int myMaxValue=2_147_483_647;
	int myTotal = (myMinValue/2);
	System.out.println("myTotal = " + myTotal);

	//byte has a width of 8
	byte myByteValue = -128;
	byte myNewByteValue = (byte) (myByteValue/2);
	System.out.println("myNewByteValue = " + myNewByteValue);

	//short has a width of 16
	short myShortValue = 32767;
	short myNewShortValue = (short) (myShortValue /2);
	System.out.println("myNewShortValue = " + myNewShortValue);

	//long has a width of 64 bit of memory;
	long myLongMinValue = -9_223_372_036_854_775_808L;
	long myLongMaxValue = 9_223_372_036_854_775_807L;

	long myNewLongValue = myLongMaxValue / (12345678);
	System.out.println("myNewLongValue = " + myNewLongValue);

    }
}
#out#
myTotal = -1073741824
myNewByteValue = -64
myNewShortValue = 16383
myNewLongValue = 747093196246

###########################################
package com.larisa;

public class Main {

    public static void main(String[] args) {
        //
	    char myChar = '\u00A9';
        System.out.println("Unicode output was;" + myChar);
        char registerSymbol = '\u00AE';
        System.out.println("Register symbol;" + registerSymbol);

        boolean myBoolean = false;
        boolean myMale = true;

    }
}
#out###
Unicode output was;©
Register symbol;®
#############################
package com.larisa;

public class Main {

    public static void main(String[] args) {
        printDayOfTheWeek(-1);
        printDayOfTheWeek(0);
        printDayOfTheWeek(1);
        printDayOfTheWeek(2);
        printDayOfTheWeek(3);
        printDayOfTheWeek(4);
        printDayOfTheWeek(5);
        printDayOfTheWeek(6);
    }
    private static  void printDayOfTheWeek(int day){
//        if (day==0) {
//            System.out.println("Sunday");
//        }else if(day==1) {
//            System.out.println("Monday");
//        }else if(day==2) {
//            System.out.println("Tuesday");
//        }else if(day==3) {
//            System.out.println("Wednesday");
//        }else if(day==4) {
//            System.out.println("Thusday");
//        }else if(day==5) {
//            System.out.println("Friday");
//        }else if(day==6) {
//            System.out.println("Saturday");
//        }

        switch (day){
            case 0:
                System.out.println("Sunday");
                break;
            case 1:
                System.out.println("Monday");
                break;
            case 2:
                System.out.println("Tuesday");
                break;
            case 3:
                System.out.println("Wednesday");
                break;
            case 4:
                System.out.println("Thusday");
                break;
            case 5:
                System.out.println("Friday");
                break;
            case 6:
                System.out.println("Saturday");
                break;
        }

    }
}
#out###Sunday
Monday
Tuesday
Wednesday
Thusday
Friday
Saturday
##############
public class DecimalComparator {
    public static boolean areEqualByThreeDecimalPlaces(double number1,double number2){
        int a1 = (int)(number1*1000);
        int b1 = (int)(number2*1000);
        if(a1 == b1){
            return true;
        }
        else{
            return false;
        }
    }
}

public class Main {
    public static void main(String[] args) {
        System.out.println(DecimalComparator.areEqualByThreeDecimalPlaces(111,711));
        System.out.println(DecimalComparator.areEqualByThreeDecimalPlaces(2345,767676));
        System.out.println(DecimalComparator.areEqualByThreeDecimalPlaces(-2345,2345));
        System.out.println(DecimalComparator.areEqualByThreeDecimalPlaces(2345,2345));
    }
}
######out###
false
false
false
true
####################
public class IntEqualityPrinter {
    public static void printEqual(int a, int b, int c){
        if((a < 0) || (b < 0) || (c <0)) {
            System.out.println("Invalid Value");
        }else
        if((a == b) && (b == c)) {
            System.out.println("All numbers are equal");
        }else
        if((a !=b) && (b !=c) && (a != c) ) {
            System.out.println("All numbers are different");
        } else{
            System.out.println("Neither all are equal or different");
        }

    }
}
public class Main {
    public static  void  main(String[] arg) {
        IntEqualityPrinter.printEqual(1, 1, 1);
        IntEqualityPrinter.printEqual(1, 1, 2);
        IntEqualityPrinter.printEqual(-1, -1, -1);
        IntEqualityPrinter.printEqual(1, 2, 3);
    }
}
##########out#############
All numbers are equal
Neither all are equal or different
Invalid Value
All numbers are different
####################################
public class EqualSumChacker {
    public static boolean hasEqualSum(int a, int b, int c) {
        return (a + b == c);
    }
}

public class Main {
    public static  void  main(String[] arg){
        System.out.println(EqualSumChacker.hasEqualSum(-1,-1,-1));
        System.out.println(EqualSumChacker.hasEqualSum(1, 1, 2));
        System.out.println(EqualSumChacker.hasEqualSum(1, -1, 0));
        System.out.println(EqualSumChacker.hasEqualSum(-3,-4,-5));
        System.out.println(EqualSumChacker.hasEqualSum(-4,-4,-4));
    }
}
#############out#############
false
true
true
false
false
##############################
//Write a method named getEvenDigitSum with one parameter of type int called number.
//        The method should return the sum of the even digits within the number.
//        If the number is negative, the method should return -1 to indicate an invalid value.
//        EXAMPLE INPUT/OUTPUT:
//        * getEvenDigitSum(123456789); → should return 20 since 2 + 4 + 6 + 8 = 20
//        * getEvenDigitSum(252); → should return 4 since 2 + 2 = 4
//        * getEvenDigitSum(-22); → should return -1 since the number is negative
//        NOTE: The method getEvenDigitSum should be defined as public static
//                like we have been doing so far in the course.
//        NOTE: Do not add a main method to the solution code.

public class EvenDigitSum {
    public static int getEvenDigitSum(int number){
        if (number < 0){
            return -1;
        }
        int sum = 0;
        while(number > 0){
            int digit = number % 10;
            if (number % 2 == 0) {
                sum += digit;
            }
            number /= 10;
        }
        return sum;
    }
}

public class Main {
    public static  void  main(String[] arg){
        System.out.println(EvenDigitSum.getEvenDigitSum(4567));
        System.out.println(EvenDigitSum.getEvenDigitSum(44567));
        System.out.println(EvenDigitSum.getEvenDigitSum(45367));
        System.out.println(EvenDigitSum.getEvenDigitSum(5));
        System.out.println(EvenDigitSum.getEvenDigitSum(-45267));
    }
}

###out###
10
14
10
0
-1
########################
//Write a method named printFactors with one parameter of type int named number.
//        If number is < 1, the method should print "Invalid Value".
//        The method should print all factors of the number. A factor of a number is an integer which divides that
//        number wholly (i.e. without leaving a remainder).
//        For example, 3 is a factor of 6 because 3 fully divides 6 without leaving a remainder. In other words 6 / 3 = 2.
//        EXAMPLE INPUT/OUTPUT:
//        * printFactors(6); → should print 1 2 3 6
//        * printFactors(32); → should print 1 2 4 8 16 32
//        * printFactors(10); → should print 1 2 5 10
//        * printFactors(-1); → should print "Invalid Value" since number is < 1
//        HINT: Use a while or for loop.
//        NOTE: When printing numbers, each number can be in its own line. They don't have to be separated by a space.
//        For example, the printout for printFactors(10); can be:
//        1
//        2
//        5
//        10
//        NOTE: The method printFactors should be defined as public static like we have been doing so far in the course.

public class FactorPrinter {
    public static void printFactors (int number) {
        if (number < 1) {
            System.out.println("Invalid Value");
        } else {
            int value = 1;
            while (value <= number) {
                if (number % value == 0) {
                    System.out.println(value);
                    value++;
                    continue;
                }
                value++;
            }
        System.out.println("****************************");
        }
    }
}

public class Main {
    public static  void  main(String[] arg) {
        FactorPrinter.printFactors(6);
        FactorPrinter.printFactors(32);
        FactorPrinter.printFactors(10);
        FactorPrinter.printFactors(-1);
    }
}
######out###
1
2
3
6
****************************
1
2
4
8
16
32
****************************
1
2
5
10
****************************
Invalid Value

########################################Process finished with exit code 0
//Write a method named sumFirstAndLastDigit with one parameter of type int called number.
//        The method needs to find the first and the last digit of the parameter number passed to the method, using a loop and return the sum of the first and the last digit of that number.
//        If the number is negative then the method needs to return -1 to indicate an invalid value.
//        Example input/output
//        * sumFirstAndLastDigit(252); → should return 4, the first digit is 2 and the last is 2 which gives us 2+2
//        and the sum is 4.
//        * sumFirstAndLastDigit(257); → should return 9, the first digit is 2 and the last is 7 which gives us 2+7
//        and the sum is 9.
//        * sumFirstAndLastDigit(0); → should return 0, the first digit and the last digit is 0 since we only
//        have 1 digit, which gives us 0+0 and the sum is 0.
//        * sumFirstAndLastDigit(5); → should return 10, the first digit and the last digit is 5 since we only
//        have 1 digit, which gives us 5+5 and the sum is 10.
//        * sumFirstAndLastDigit(-10); → should return -1, since the parameter is negative and needs to be positive.
//        NOTE: The method sumFirstAndLastDigit needs to be defined as public static like we have been doing
//        so far in the course.
//        NOTE: Do not add a  main method to solution code.

public class FirstAndLastDigitSum {
    public static int sumFirstAndLastDigit(int number) {
        if (number < 0) {
            return -1;
        }

        int lastDigit = number % 10;

        int firstDigit = number;
        while (firstDigit >= 10) {
            firstDigit /= 10;
        }
        return lastDigit + firstDigit;
    }
}
public class Main {
    public static  void  main(String[] arg) {
        System.out.println(FirstAndLastDigitSum.sumFirstAndLastDigit(252));
        System.out.println(FirstAndLastDigitSum.sumFirstAndLastDigit(257));
        System.out.println(FirstAndLastDigitSum.sumFirstAndLastDigit(0));
        System.out.println(FirstAndLastDigitSum.sumFirstAndLastDigit(-16));
        System.out.println(FirstAndLastDigitSum.sumFirstAndLastDigit(5));
    }
    }
####
2019.1.1\bin" -Dfile.encoding=UTF-8 -classpath "C:\Users\Larisa Vasukov\Desktop\JavaProgram\FirstAndLastDigitSum\out\production\FirstAndLastDigitSum" Main
4
9
0
-1
10
##############
package com.larisa;

public class Main {

    public static void main(String[] args) {
    	// width of int = 32 (4 bytes).
		int myIntValue = 5 /2;
		// width of int = 32 (4 bytes).
		float myFoatValue = 5.f /3f;
		// width of int = 64 (8 bytes).
		double myDoubleValue = 5.d /3d;
		System.out.println("myIntValue = " + myIntValue);
		System.out.println("myFoatValue = " + myFoatValue);
		System.out.println("myDoubleValue = " + myDoubleValue);

		//Convert point to kg
		//Notes: 1poun = 0.45359237 kk
		double numPounts = 200d;
		double convertedKg = numPounts * 0.45359237d;
		System.out.println("Kg = " + convertedKg);
		double pi = 3.14d;
		System.out.println("pi = " + pi);

	}
}
##########################
package com.larisa;

public class Main {

    public static void main(String[] args) {
        System.out.println("10000 at 2% interest = " + calculateInterest(10000.0, 2.0));
        System.out.println("10000 at 2% interest = " + calculateInterest(10000.0, 3.0));
        System.out.println("10000 at 2% interest = " + calculateInterest(10000.0, 4.0));
        System.out.println("10000 at 2% interest = " + calculateInterest(10000.0, 5.0));
        for (int i = 0; i < 5; i++) {
            System.out.println("Loop " + i + " hello");
        }
        for (int i = 2; i < 9; i++) {
            System.out.println("10000 at " + i + "% interest = " + String.format("%2f", calculateInterest(10000.0, i)));
        }
        System.out.println("*********************************************");
        for (int i = 8; i >=2; i--) {
            System.out.println("10000 at " + i + "% interest = " + String.format("%2f", calculateInterest(10000.0, i)));
        }
        int count =0;
        for (int i=10; 1<50; i++){
            if (isPrime(i)){
                count++;
                System.out.println("Count number " + i + " count: " + count);
                if(count == 6);{
                    System.out.println("Existing from a loop");
                    break;
                }
            }
        }
    }
    public static boolean isPrime(int n) {
        if(n == 1){
            return false;
        }
        for(int i=2; i <= (long)Math.sqrt(n); i++){
            if(n % i ==0){
                return false;
            }
        }
        return true;
    }
    public static double calculateInterest(double amount, double interestRate) {
        return(amount*(interestRate/100));
    }
}
##############################
public class GreatestCommonDivisor {
    public static int getGreatestCommonDivisor(int first, int second) {
        if (first < 10 || second < 10) {
            return -1;
        }
// ******************first*************
//        while(second !=0) {
//            int remainder = first % second;
//            first = second;
//            second = remainder;
//        }
//        return first;
//        *************end of first *****************
// ************************second********************
//        int a = first;
//        int b = second;
//
//        while (a <= b) {
//            int bob = (first % a);
//            if (bob == 0) {
//                int sam = (second % a);
//                if (sam == 0) {
//                    return a;
//                }
//            }
//            a--;
//        }
//        while (b < a) {
//            int fr = (second % b);
//            if (fr == 0) {
//                int fe = (first % b);
//                if (fe == 0) {
//                    return b;
//                }
//            }
//            b--;
//        }
//        return -3;
//  **********end of secong******************
        int gcd = 0;
        for(int i =1;i <= first && i <= second;++i){
            if (first% i == 0 && second % i== 0){
                gcd = i;
            }
        }
        return gcd;
    }
}

public class Main {
    public static  void  main(String[] arg) {
        System.out.println(GreatestCommonDivisor.getGreatestCommonDivisor(25, 15));
        System.out.println(GreatestCommonDivisor.getGreatestCommonDivisor(12, 30));
        System.out.println(GreatestCommonDivisor.getGreatestCommonDivisor(9, 18));
        System.out.println(GreatestCommonDivisor.getGreatestCommonDivisor(81, 153));
    }
}
****************
5
6
-1
9
############################
package com.larisa;

public class Main {

    public static void main(String[] args) {
		boolean gameOver = true;
		int score = 400;
		int levelCompleted = 5;
		int bonus = 100;

//		if(score < 5000 && score > 1000){
//			System.out.println("Your score was less than 5000 but greater than 1000");
//		}else if (score < 1000) {
//			System.out.println("Your score was less than 1000");
//		}else {
//			System.out.println("Got here");
//		}
		if(gameOver == true){
			int  finalScore = score + (levelCompleted * bonus);
			finalScore += 1000;
			System.out.println(("Your final score was " + finalScore));
		}

		score = 1000;
		levelCompleted = 8;
		bonus = 200;
		if(gameOver) {
			int finalScore = score + (levelCompleted * bonus);
			System.out.println(("Your final score was " + finalScore));
		}
	}
}

*****************
Your final score was 1900
Your final score was 2600
##########
package com.larisa;

public class Main {

    public static void main(String[] args) {
        int myVariable=50;
        if (myVariable==50){
            System.out.println("myVariable=50");
        }
        myVariable++;
        myVariable--;
        System.out.println("This is a test");

        System.out.println("This is" +
                " another" +
                " still more.");

        int anotheVariable = 50;myVariable--;System.out.println("this is another one");

        // false, true, null
    }
}
***********************
myVariable=50
This is a test
This is another still more.
this is another one
#####################
//Write a method named hasSameLastDigit with three parameters of type int.
//        Each number should be within the range of 10 (inclusive) - 1000 (inclusive).
//        If one of the numbers is not within the range, the method should return false.
//        The method should return true if at least two of the numbers share the same rightmost digit;
//otherwise, it should return false.
//        EXAMPLE INPUT/OUTPUT:
//        * hasSameLastDigit (41, 22, 71); → should return true since 1 is the rightmost digit in numbers 41 and 71
//        * hasSameLastDigit (23, 32, 42); → should return true since 2 is the rightmost digit in numbers 32 and 42
//        * hasSameLastDigit (9, 99, 999); → should return false since 9 is not within the range of 10-1000
//        Write another method named isValid with one parameter of type int.
//        The method needs to return true if the number parameter is in range of 10(inclusive) - 1000(inclusive),
//        otherwise return false.
//        EXAMPLE INPUT/OUTPUT
//        * isValid(10); → should return true since 10 is within the range of 10-1000
//        * isValid(468); → should return true since 468 is within the range of 10-1000
//        * isValid(1051); → should return false since 1051 is not within the range of 10-1000
//        NOTE: All methods need to be defined as public static as we have been doing so far in the course.
//        NOTE: Do not add a main method to the solution code.

public class LastDigitChecker {
    public static boolean hasSameLastDigit(int firstNumber, int secondNumber, int thirdNumber){
        while(isValid(firstNumber) && isValid(secondNumber) && isValid(thirdNumber)){
            firstNumber %= 10;
            secondNumber %= 10;
            thirdNumber %= 10;
            if(firstNumber == secondNumber || firstNumber == thirdNumber|| secondNumber== thirdNumber){
                return true;
            }
        }
        return false;
    }
    public static boolean isValid(int number){

        if(number < 10 || number > 1000){
            return false;
        }
        return true;
    }
}
public class Main {
    public static  void  main(String[] arg) {
        System.out.println(LastDigitChecker.hasSameLastDigit(41, 22, 71));
        System.out.println(LastDigitChecker.hasSameLastDigit(23, 32, 42));
        System.out.println(LastDigitChecker.hasSameLastDigit(9, 99, 999));
        System.out.println(LastDigitChecker.hasSameLastDigit(12,34,1051));
        System.out.println(LastDigitChecker.isValid(10));
        System.out.println(LastDigitChecker.isValid(148));
        System.out.println(LastDigitChecker.isValid(468));
        System.out.println(LastDigitChecker.isValid(1051));
    }
    }
*****************
true
true
false
false
true
true
true
false
##########################################
//Write a method isLeapYear with a parameter of type int named year.
//The parameter needs to be greater than or equal to 1 and less than or equal to 9999.
//If the parameter is not in that range return false.
//Otherwise, if it is in the valid range, calculate if the year is a leap year and return true
// if it is a leap year, otherwise return false.
//To determine whether a year is a leap year, follow these steps:
//1. If the year is evenly divisible by 4, go to step
//2. Otherwise, go to step 5.2. If the year is evenly divisible by 100, go to step
//3. Otherwise, go to step 4.3. If the year is evenly divisible by 400, go to step
//4. Otherwise, go to step 5.4. The year is a leap year (it has 366 days). The method isLeapYear needs to return true.
//5. The year is not a leap year (it has 365 days). The method isLeapYear needs to return false.
//The following years are not leap years:
//1700, 1800, 1900, 2100, 2200, 2300, 2500, 2600
//This is because they are evenly divisible by 100 but not by 400.
//The following years are leap years:
//1600, 2000, 2400
//This is because they are evenly divisible by both 100 and 400.
//Examples of input/output:
//* isLeapYear(-1600); → should return false since the parameter is not in range (1-9999)
//* isLeapYear(1600); → should return true since 1600 is a leap year
//* isLeapYear(2017); → should return false since 2017 is not a leap year
//* isLeapYear(2000);  → should return true because 2000 is a leap year
//NOTE: The method isLeapYear needs to be defined as public static like we have been doing so far in the course.

public class LeapYear {
    public static boolean isLeapYear(int year){
        if(year >= 1 && year <= 9999){
            if(year % 4 == 0){
                if(year % 100 == 0 && year % 400 == 0){
                    return true;
                }else if(year % 100 != 0 && year % 400 != 0){
                    return true;
                }else return false;
            }else return false;
        }else return false;
    }
}

public class Main {
    public static  void  main(String[] arg) {
        System.out.println(LeapYear.isLeapYear(1600));
    }
}
**************
true
####################
package com.larisa;

public class Main {

    public static void main(String[] args) {

        boolean gameOver = true;
        int score = 800;
        int levelCompleted = 5;
        int bonus = 100;

        int highScore = calculateScore(gameOver, score, levelCompleted, bonus);
        System.out.println("Your final score was " + highScore);

        score = 1000;
        levelCompleted = 8;
        bonus = 200;

        //        Call this metod and return following positions:
//        1500,900,400,50

        int highScorePosition = calculateHighScorePosition(1500);
        displayHighScorePosition("Hi Tim", highScorePosition);

        highScorePosition = calculateHighScorePosition(90);
        displayHighScorePosition("Hi Bob", highScorePosition);
        highScorePosition = calculateHighScorePosition(450);
        displayHighScorePosition("Hi Rob", highScorePosition);
        highScorePosition = calculateHighScorePosition(900);
        displayHighScorePosition("Hi Dan", highScorePosition);
    }

    public static void displayHighScorePosition(String playerName, int highScorePosition) {
        System.out.println(playerName + " managed to get into position " +
                highScorePosition + " on the high score table.");
    }

    public static int calculateHighScorePosition(int playerScore) {
        if (playerScore >= 1000) {
            return 1;
        } else if (playerScore >= 500 && playerScore < 1000) {
            return 2;
        } else if (playerScore >= 100 && playerScore < 500) {
            return 3;
        } else {
            return 4;
        }
    }

    public static int calculateScore(boolean gameOver, int score, int levelCompleted, int bonus) {

        if (gameOver) {
            int finalScore = score + (levelCompleted * bonus);
            finalScore += 2000;
            System.out.println(("Your final score was " + finalScore));
            return finalScore;
        } else {
            System.out.println("It not gameOver");
            return -1;
        }
    }
}
******************
Your final score was 3300
Your final score was 3300
Hi Tim managed to get into position 1 on the high score table.
Hi Bob managed to get into position 4 on the high score table.
Hi Rob managed to get into position 3 on the high score table.
Hi Dan managed to get into position 2 on the high score table.
###########################
public class MegaBytesConverte {
    public static void printMegaBytesAndKiloBytes(int kiloBytes) {
        if (kiloBytes < 0) {
            System.out.println("Invalid Value");
        } else {
            int mbValue = kiloBytes / 1024;
            int kbValue = kiloBytes - (1024 * mbValue);
            System.out.println(kiloBytes + " KB = " + mbValue + " MB and " + kbValue+" KB");
        }
    }
}
public class Main {
    public static  void  main(String[] arg){
        MegaBytesConverte.printMegaBytesAndKiloBytes(2500);
        MegaBytesConverte.printMegaBytesAndKiloBytes(-1024);
        MegaBytesConverte.printMegaBytesAndKiloBytes(5000);
        }
}
***
2500 KB = 2 MB and 452 KB
Invalid Value
5000 KB = 4 MB and 904 KB
#################

package com.larisa;

public class Main {
    public static void main(String[] args) {
        int newScore = calculateStore("Tim", 500);
        System.out.println("New score is: " + newScore);
        calculateStore(75);
        calculateStore();
        double centimeters = calcFeetAndInchesToCentimeters(6, 0);
        if(centimeters <0.0){
            System.out.println("Invalid parameters");
        }

    calcFeetAndInchesToCentimeters(157);
    }


    public static double calcFeetAndInchesToCentimeters(double feet, double inches) {
        if ((feet < 0) || ((inches < 0) && ((inches > 12)))) {
            System.out.println("Invalid feet or inches parameters");
            return -1;
        }
        double centimeters = (feet * 12) * 2.54;
        centimeters += inches * 2.54;
        System.out.println(feet + " feet, " + inches + " inches = " + centimeters + " cm");
        return centimeters;
    }

    public static double calcFeetAndInchesToCentimeters(double inches) {
        if (inches < 0) {
            System.out.println("Invalid feet or inches parameters");
            return -1;
        }
        double feet = (int) inches / 12;
        double remainingInches = inches % 12;
        System.out.println(" feet, " + feet + " inches = " + remainingInches);
        return calcFeetAndInchesToCentimeters(feet, remainingInches);
    }
    public static int calculateStore(String playerName, int score) {
        System.out.println("Player:" + playerName + "   score:" + score + " point");
        return score * 1000;
    }
    public static int calculateStore(int score) {
        System.out.println("Unname player scored:" + score + " point");
        return score * 1000;
    }
    public static int calculateStore() {
        System.out.println("No player name, no player score");
        return 0;
    }
}

*************************
Player:Tim   score:500 point
New score is: 500000
Unname player scored:75 point
No player name, no player score
6.0 feet, 0.0 inches = 182.88 cm
 feet, 13.0 inches = 1.0
13.0 feet, 1.0 inches = 398.78000000000003 cm
####################################
public class MinuteHoureDay {
    public static void printYearsAndDays(long minutes) {
        if (minutes < 0) {
            System.out.println("Invalid Value");
        } else {
            long hours = minutes / 60;
            long days = hours / 24;
            long years = days / 365;
            long remainingDays = (days % 365);
            System.out.println(minutes + " min = " + years + " y and " + remainingDays + " d");
        }
    }
}

public class Main {
    public static  void  main(String[] arg){
        MinuteHoureDay.printYearsAndDays(525600);
        MinuteHoureDay.printYearsAndDays(1051200);
        MinuteHoureDay.printYearsAndDays(106767);
        MinuteHoureDay.printYearsAndDays(561622);
    }
}
**********************
525600 min = 1 y and 0 d
1051200 min = 2 y and 0 d
106767 min = 0 y and 74 d
561622 min = 1 y and 25 d
############################################
//        A year is a leap year if it is divisible by 4 but not by 100, or it is divisible by 400.
//        ​NOTE:  The solution to the Leap Year coding exercise earlier in the course created the isLeapYear method.
//        You can use that solution if you wish.
//        Write another method getDaysInMonth with two parameters month and year.  ​Both of type int.
//        If parameter month is < 1 or > 12 return -1. ​
//        If parameter year is < 1 or > 9999 then return -1.
//        This method needs to return the number of days in the month. Be careful about leap years they have 29 days
//        in month 2 (February).
//        You should check if the year is a leap year using the method isLeapYear described above.
public class NumberOfDaysInMonth {
    public static boolean isLeapYear(int year) {
        return year >= 1
                && year <= 9999
                && year % 4 == 0
                && (year % 100 != 0
                || year % 400 == 0);
    }

    public static int getDaysInMonth(int month, int year) {
        if (month < 1 || month > 12 || year < 1 || year > 9999) {
            return -1;
        }
        switch (month) {
            case 4: case 6: case 9: case 11:
                return 30;
            case 2:
                return isLeapYear(year) ? 29 : 28;
            default:
                return 31;
        }
    }
}

public class Main {
    public static  void  main(String[] arg){
        System.out.println(NumberOfDaysInMonth.isLeapYear(-1600));
        System.out.println(NumberOfDaysInMonth.isLeapYear(1600));
        System.out.println(NumberOfDaysInMonth.isLeapYear(2017));
        System.out.println(NumberOfDaysInMonth.isLeapYear(2000));
        System.out.println(NumberOfDaysInMonth.getDaysInMonth(1, 2020));
        System.out.println(NumberOfDaysInMonth.getDaysInMonth(2, 2020));
        System.out.println(NumberOfDaysInMonth.getDaysInMonth(2, 2018));
        System.out.println(NumberOfDaysInMonth.getDaysInMonth(-1, 2020));
    }
}
**************************
false
true
false
true
31
29
28
-1
###################################
package com.larisa;

public class Main {

    public static void main(String[] args) {
        int result = 1+2;
        System.out.println("1+2=" + result);
        result ++;
        System.out.println("Result is now = " + result);
        result --;
        System.out.println("Result is now = " + result);
        result += 2;
        System.out.println("Result is now = " + result);
        result *= 10;
        System.out.println("Result is now = " + result);
        result -= 10;
        System.out.println("Result is now = " + result);
        result /= 4;
        System.out.println("Result is now = " + result);
        result = result /2;
        System.out.println("Result is now = " + result);
        result = result * 2;
        System.out.println("Result is now = " + result);
        result = result % 3;
        System.out.println("Result is now = " + result);

        boolean isAlen = false;
        if (isAlen ==true)
            System.out.println("It is Alen!");

        int topScore = 82;
        if (topScore < 100)
            System.out.println("You got a High Scool");

        int secondTopScore = 81;
        if ((topScore >  secondTopScore) && (topScore < 100))
            System.out.println("You are in [81;100]");

        if ((topScore >  90) || (secondTopScore < 90))
            System.out.println("one of these tests is true");

        int newValiue = 50;
        if (newValiue == 50)
            System.out.println("50 is True");

        boolean isCar = true;
        if (isCar != true)
            System.out.println("It is a car");

        isCar = true;
         boolean wasCar = isCar ? true: false;
         if(wasCar)
             System.out.println("wasCar is true");

         double myFiirstValue = 20d;
         double mySEcondValue = 80d;
         double myTotal = (myFiirstValue + mySEcondValue) * 25;
         System.out.println("myTotal = " + myTotal);
         double theReminder = myTotal %40;
         System.out.println("theReminder = " + theReminder);
         if (theReminder <= 20)
             System.out.println("Total was over the limit");

    }
}
*****************
1+2=3
Result is now = 4
Result is now = 3
Result is now = 5
Result is now = 50
Result is now = 40
Result is now = 10
Result is now = 5
Result is now = 10
Result is now = 1
You got a High Scool
You are in [81;100]
one of these tests is true
50 is True
wasCar is true
myTotal = 2500.0
theReminder = 20.0
Total was over the limit

Process finished with exit code 0

#####################################
public class Palindrome {

    public static boolean isPalindrome(int number) {
        int originalNumber = number;
        int reverse = 0;

        while (number !=0) {
            int lastDigit = number % 10;
            reverse *= 10;
            reverse += lastDigit;
            number /= 10;

        }
        if (reverse == originalNumber) {
            return true;
        } else {
            return false;
        }
    }
}

public class Main {
    public static  void  main(String[] arg) {
        System.out.println(Palindrome.isPalindrome(1123454321));
        System.out.println(Palindrome.isPalindrome(123404321));
        System.out.println(Palindrome.isPalindrome(12344321));
        System.out.println(Palindrome.isPalindrome(-123454321));
        System.out.println(Palindrome.isPalindrome(12341));

    }
}
************************
false
true
true
true
false
############################
//        A perfect number is a positive integer which is equal to the sum of its proper positive divisors.
//        * isPerfectNumber(28); should return true since its proper divisors are 1, 2, 4, 7, 14
//        and the sum is 1 + 2 + 4 + 7 + 14 = 28

public class PerfectNumber {
    public static boolean isPerfectNumber (int number){

        if (number <1){
            return false;
        }
        else{
            int value = 1;
            int sum = 0;
            while (value < number){
                if (number % value == 0){
                    sum += value;
                    value++;
                    continue;
                }
                value++;
            }
            if (number == sum){
                return true;
            }
            return false;
        }
    }
}

public class Main {
    public static  void  main(String[] arg) {
        System.out.println(PerfectNumber.isPerfectNumber(6));
        System.out.println(PerfectNumber.isPerfectNumber(28));
        System.out.println(PerfectNumber.isPerfectNumber(5));
        System.out.println(PerfectNumber.isPerfectNumber(-1));
    }
    }
*****************
true
true
false
false
###################
//The cats spend most of the day playing. In particular, they play if the temperature is between 25 and 35 (inclusive).
// Unless it is summer, then the upper limit is 45 (inclusive) instead of 35.
//        Write a method isCatPlaying that has 2 parameters. Method needs to return true if the cat is playing,
//        otherwise return false
//        1st parameter should be of type boolean and be named summer it represents if it is summer.
//        2nd parameter represents the temperature and is of type int with the name temperature.

public class PlayingCat {
    public static boolean isCatPlaying(boolean summer, int temperature){
        if(summer && (temperature>=25 && temperature<=45)){
            return true;
        } else if(!summer && (temperature>=25 && temperature<=35)){
            return true;
        } else {
            return false;
        }
    }
}

public class Main {
    public static  void  main(String[] arg){
        System.out.println(PlayingCat.isCatPlaying(true, 10));
        System.out.println(PlayingCat.isCatPlaying(false, 36));
        System.out.println(PlayingCat.isCatPlaying(false, 35));
        System.out.println(PlayingCat.isCatPlaying(true, 25));
    }
}

*********************

###############################
package com.larisa;

public class Main {
    private static final String INVALID_VALUE_MESSAGE = "Invalid value";

    public static void main(String[] args) {
        System.out.println(getDurationString(65, 45));
        System.out.println(getDurationString( 4567L));
        System.out.println(getDurationString( -20));
        System.out.println(getDurationString( 33,8));
    }

    public static String getDurationString(long minutes, long seconds) {
        if ((minutes < 0) || (seconds < 0) || (seconds > 59)) {
            return INVALID_VALUE_MESSAGE;
        }
        long hours = minutes / 60;
        long remainingMinutes = minutes % 60;

        String hoursString = hours + "h";
        if (hours < 10){
            hoursString = "0" + hoursString;
        }

        String minutesString = remainingMinutes + "m";
        if (remainingMinutes < 10){
            minutesString = "0" + minutesString;
        }
        String secondsString = seconds + "s";
        if (seconds < 10){
            secondsString = "0" + secondsString;
        }

        return hoursString + " " + minutesString + " " + secondsString + " ";

//        return hours + "h " + remainingMinutes + "m " + seconds + "s ";
    }

    public static String getDurationString(long seconds) {
        if (seconds < 0) {
            return INVALID_VALUE_MESSAGE;
        }
        long minutes = seconds / 60;
        long remainingSeconds = seconds % 60;
        if (minutes >= 60) {
            long hours = minutes / 60;
            long remainingMinutes = minutes % 60;
            return hours + "h " + remainingMinutes + "m " + remainingSeconds + "s ";
        } else {
            return minutes + "m " + remainingSeconds + "s ";
        }
    }
}
*********************
package com.larisa;

public class Main {
    private static final String INVALID_VALUE_MESSAGE = "Invalid value";

    public static void main(String[] args) {
        System.out.println(getDurationString(65, 45));
        System.out.println(getDurationString( 4567L));
        System.out.println(getDurationString( -20));
        System.out.println(getDurationString( 33,8));
    }

    public static String getDurationString(long minutes, long seconds) {
        if ((minutes < 0) || (seconds < 0) || (seconds > 59)) {
            return INVALID_VALUE_MESSAGE;
        }
        long hours = minutes / 60;
        long remainingMinutes = minutes % 60;

        String hoursString = hours + "h";
        if (hours < 10){
            hoursString = "0" + hoursString;
        }

        String minutesString = remainingMinutes + "m";
        if (remainingMinutes < 10){
            minutesString = "0" + minutesString;
        }
        String secondsString = seconds + "s";
        if (seconds < 10){
            secondsString = "0" + secondsString;
        }

        return hoursString + " " + minutesString + " " + secondsString + " ";

//        return hours + "h " + remainingMinutes + "m " + seconds + "s ";
    }

    public static String getDurationString(long seconds) {
        if (seconds < 0) {
            return INVALID_VALUE_MESSAGE;
        }
        long minutes = seconds / 60;
        long remainingSeconds = seconds % 60;
        if (minutes >= 60) {
            long hours = minutes / 60;
            long remainingMinutes = minutes % 60;
            return hours + "h " + remainingMinutes + "m " + remainingSeconds + "s ";
        } else {
            return minutes + "m " + remainingSeconds + "s ";
        }
    }
}
******************
01h 05m 45s 
1h 16m 7s 
Invalid value
00h 33m 08s 
##############################
/Write a method named hasSharedDigit with two parameters of type int.
//        Each number should be within the range of 10 (inclusive) - 99 (inclusive).
//        If one of the numbers is not within the range, the method should return false.
//        The method should return true if there is a digit that appears in both numbers, such as 2 in 12 and 23;
//            otherwise, the method should return false.
//        EXAMPLE INPUT/OUTPUT:
//        * hasSharedDigit(12, 23); → should return true since the digit 2 appears in both numbers
//        * hasSharedDigit(9, 99); → should return false since 9 is not within the range of 10-99
//        * hasSharedDigit(15, 55); → should return true since the digit 5 appears in both numbers
//        NOTE: The method hasSharedDigit should be defined as public static like we have been doing so far in the course.
//        NOTE: Do not add a main method to the solution code.

public class SharedDigit {
    public static boolean hasSharedDigit(int number1, int number2) {

        if (number1 < 10 || number1 > 99 || number2 < 10 || number2 > 99) {

            return false;
        }

        if(number1 > 0 && number2 > 0) {

            int lastDigit1 = number1 % 10;
            int lastDigit2 = number2 % 10;

            int firstDigit1 = number1 /= 10;
            int firstDigit2 = number2 /= 10;

            if (lastDigit1 == lastDigit2 || firstDigit1 == firstDigit2 || lastDigit1 == firstDigit2|| lastDigit2 == firstDigit1) {

                return true;
            }


        }

        return false;
    }
}
public class Main {
    public static  void  main(String[] arg) {
        System.out.println(SharedDigit.hasSharedDigit(12, 23));
        System.out.println(SharedDigit.hasSharedDigit(10, 100));
        System.out.println(SharedDigit.hasSharedDigit(15, 55));
        System.out.println(SharedDigit.hasSharedDigit(15, -55));
        System.out.println(SharedDigit.hasSharedDigit(15, 35));

    }
    }
**********************
true
false
true
false
true
#################################
package com.larisa;

public class Main {

    public static void main(String[] args) {
        int calcSum = 0;
        int count = 0;
        for (int i = 1; i < 1000; i++) {
            if ((i % 3 == 0) && (i % 5 == 0)){
                calcSum += i;
                count++;
                if(count == 5){
                    System.out.println(" calcSum = " + calcSum);
                    break;
                }
            }
        }

    }
}

***8
 calcSum = 225
 ########################
 package com.larisa;

public class Main {

    public static void main(String[] args) {
        System.out.println("125:" + sumDigits(125));
        System.out.println("-125:" + sumDigits(-125));
        System.out.println("125555:" + sumDigits(125555));
        System.out.println("16625:" + sumDigits(16625));
        System.out.println("1229295:" + sumDigits(1229295));
    }
    private static int sumDigits(int number){
        if(number <10){
            return -1;
        }
        int sum=0;
//        125/10 -> 12/10-> 1+2 + 5
        while(number >0){
            int digit = number % 10;
            sum += digit;
            number /=10;
        }
        return sum;
    }
}
*****
125:8
-125:-1
125555:23
16625:20
1229295:30
################################
//Write a method called isOdd with an int parameter and call it number. The method needs to return a boolean.
//        Check that number is > 0, if it is not return false.
//        If number is odd return true, otherwise  return false.
//        Write a second method called sumOdd that has 2 int parameters start and end, which represent a range of numbers.
//        The method should use a for loop to sum all odd numbers  in that range including the end and return the sum.
//        It should call the method isOdd to check if each number is odd.
//        The parameter end needs to be greater than or equal to start and both start and end parameters
//        have to be greater than 0.
//        If those conditions are not satisfied return -1 from the method to indicate invalid input.
//        Example input/output:
//        * sumOdd(1, 100); → should return 2500
//        * sumOdd(-1, 100); →  should return -1
//        * sumOdd(100, 100); → should return 0
//        * sumOdd(13, 13); → should return 13 (This set contains one number, 13, and it is odd)
//        * sumOdd(100, -100); → should return -1
//        * sumOdd(100, 1000); → should return 247500
//        TIP: use the remainder operator to check if the number is odd
//        NOTE: Both methods  needs to be defined as public static like we have been doing so far in the course.
//        NOTE: Do not add a  main method to solution code.
public class SumOddRange {
    public static boolean isOdd(int number) {
        if (number < 0) {
            return false;
        } if (number%2!=0) {
            return true;
        }
        return false;
    }

    public static int sumOdd(int start, int end) {
        if((end < start) || (start <= 0)) {
            return -1;
        }
        int sum = 0;
        for (int i = start; i<= end; i++) {
            if (isOdd(i)) {
                sum += i;
            }
        }
        return sum;
    }
}

public class Main {
    public static  void  main(String[] arg) {
        System.out.println(SumOddRange.isOdd(123));
        System.out.println(SumOddRange.sumOdd(13, 12345));
    }
}
******************
###################
package com.larisa;

public class Main {

    public static void main(String[] args) {
//	int value = 1;
//	if (value ==1){
//        System.out.println("Value was 1");
//    }else if (value ==2){
//            System.out.println("Value was 2");
//	}else {
//            System.out.println("Value was not 1 or 2");
//	    }

        int switchValue = 2;

        switch (switchValue) {
            case 1:
                System.out.println("Value was 1");
                break;
            case 2:
                System.out.println("Value was 2");
                break;
            case 3: case 4: case 5:
                System.out.println("Value was not 3 or 4 or 5");
                System.out.println("Actually it was a " + switchValue);
                break;
            default:
                System.out.println("Other: "+ switchValue);
                break;
        }

        String month = "june";
//      switch (month.toLowerCase()){
        switch (month.toUpperCase()){
            case "January":
                System.out.println("January");
                break;
            case "JUNE":
                System.out.println("June");
                break;
            default:
                System.out.println("Not sure");
                break;
        }
        char charValue = 'A';

        switch (charValue) {
            case 'A':
                System.out.println("A was found");
                break;
            case 'B':
                System.out.println("A was found");
                break;
            case 'C':
            case 'D':
            case 'E':
                System.out.println("Actually it was a " + charValue);
                break;

            default:
                System.out.println("Other: " + charValue);
                break;
        }
    }
}
*
Value was 2
June
A was found
#####################

	//    We'll say that a number is "teen" if it is in the range 13 -19 (inclusive).
//    Write a method named hasTeen with 3 parameters of type int.
//    The method should return boolean and it needs to return true if one
//    of the parameters is in range 13(inclusive) - 19 (inclusive). Otherwise return false.
//    EXAMPLES OF INPUT/OUTPUT:
//            * hasTeen(9, 99, 19);  should return true since 19 is in range 13 - 19
//            * hasTeen(23, 15, 42);  should return true since 15 is in range 13 - 19
//            * hasTeen(22, 23, 34);  should return false since numbers 22, 23, 34 are not in range 13-19
//    Write another method named isTeen with 1 parameter of type int.
//    The method should return boolean and it needs to return true if the parameter
//    is in range 13(inclusive) - 19 (inclusive). Otherwise return false.
//    EXAMPLES OF INPUT/OUTPUT:
//            * isTeen(9);  should return false since 9 is in not range 13 - 19
//            * isTeen(13);  should return true since 13 is in range 13 - 19
//    NOTE: All methods need to be defined as public static like we have been doing so far in the course.
//    NOTE: Do not add a  main method to solution code.

 public class TeenNumberChecker {

         public static boolean hasTeen (int a, int b, int c) {

             if ((a >= 13 && a <= 19) || (b >= 13 && b <= 19) || (c >= 13 && c <= 19)) {
                 return true;
             }else{
                 return false;
             }
         }
         public static boolean isTeen (int d) {
             if (d >=13 && d <= 19){
                 return true;
             }else {
                 return false;
             }
         }
}
########################
package com.larisa;

public class Main {

    public static void main(String[] args) {
		int count = 1;
		while(count !=6){
			System.out.println("Count value is " + count);
			count ++;
		}
		for(int i=1; i!=6; i++){
			System.out.println("i value is " + i);
		}
//		count =6; Will be careful: loop and of end
//		count =1;
//		do{
//			System.out.println("Count value is do " + count);
//			count ++;
//		}while(count !=6);
//	count = 1;
//	while(true){
//		if(count ==6){
//			break;
//		}
//		System.out.println("Count value is " + count);
//		count ++;
//	}
		int number = 4;
		int finishNumber = 20;
		int evenNumberFound = 0;

		while (number <= finishNumber){
			number ++;
			if(isEventNumber(number)){
				continue;
			}
			evenNumberFound++;
			System.out.println("number: " + number);
		}
		System.out.println("evenNumberFound: " + evenNumberFound);
    }
    public static boolean isEventNumber (int number){
    	if(number % 2 != 0){
    		return true;
		}else{
			return false;
		}
	}
}
************************
Count value is 1
Count value is 2
Count value is 3
Count value is 4
Count value is 5
i value is 1
i value is 2
i value is 3
i value is 4
i value is 5
number: 6
number: 8
number: 10
number: 12
number: 14
number: 16
number: 18
number: 20
evenNumberFound: 8