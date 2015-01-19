CS143 Lab3

Jongwoo Kim 503-787-749
---------------------------------------
collaborator : Dong Jin Yu 903-692-626
---------------------------------------
Using 1 sleep day  (lab 3 deadline +1 day submition)
----------------------------------------------------
My version of lab2 had complilation error and it did not work properly,
I started the lab3 on partner's lab2.
----------------------------------------------------------------------


Query:
query starts from Parser's main and goes to Parser's start.
The Parser's start populates a catalog and computes stats for tables in the catalog using (TableStats.computeStatistics()) and computeStatistics function calls processNextStatement or handleInsertStatement instead.
In hadnleQueryStatement, a function parseQueryLogicalPlan creates a logical plan consisting table scan nodes, filter nodes, join nodes, a slect list and so forth with no ordering. The next step to constructing a logical plan is call physical plan. The physicalPlan method interacts with JoinOptimizer and TableStats to find a "good" execution order of the nodes in the logical plan. JoinOptimizer's orderJoins method chooses the "good" execution order by using stats.
Specifically, the estimations and calculations implemented in JoinOptimizer and TableStats are used to come up with the "good" plan. IntHistogram is used for each field of the table represented by TableStats.

Design Decisions:
IntHistogram: I used an array to represent a histogram. The value of each element of the array is the number of
values that fall within the corresponding range. An array is used because it is simple and fast.

TableStats: I used an array of HistogramBundles. HistogramBundle is a class I implemented, which contains the
minimum and maximum value and the histogram of the corresponding field. When TableStats is instantiated, it
initializes all the HistogramBundle elements and sets minimum and maximum values of each HistogramBundle.
Then, by using iterator, it fills up and completes all the histograms. For calculation, it uses a histogram
since it has one histogram per field.

JoinOptimizer: For orderJoins method, it iterates over each size on the outermost loop. Then, it iterates
over subsets of the size given in the current iteration. For each subset, I store the best plan in
PlanCache, so that in the next iteration of 'size+1', only the best plan for the subset of size 'size' is
considered to join with another node. Also, when joining, the algorithm chooses one node to join with the rest,
ensuring the left-deep plan.


No changes made to the API
No missing/incomplete elements
I spent around 15 hours total including the time spent for this writeup.

--------------------------------------------------------------------------------------------------
Exercise 6
1) the given query generates [a:c, c:m, m:d], as shown below.
                Join
                /   \
              Join   d
              /  \
            Join  m
            /  \
           a    c
The reason it chooses a as the left-most node is because it produces the lowest cardinality. Then c is joined with a because
c has the biggest scancost. Because the cardinality of a is very low, it's optimal to have a relation with the biggest scancost
on the right. Then, d has no join predicate with a or c, so m is chosen to join next. Lastly, d is the only relation left to join.

2)
select a.lname, a.gender
from Movie m, Costs c, Genre g, Actor a
where g.mid=m.id and m.id=c.mid and c.pid=a.id and g.genre!='Action';

Result: [g:m, m:c, c:a]
            Join
            /  \
          Join  a
          /  \
        Join  c
        /  \
       g    m

It chooses g and m to form the left-most subtree because they produce the lowest cardinality. Since the left-most subtree works
as an outer loop to the rest of joins, it's likely to require less total cost if the left-most subtree produces low cardinality.
Then, the order of g and m is chosen that way because my algorithm produces the lower estimation of cardinality that way.
Then, since a only joins with c, c is chosen to join next. Lastly, a joins with the constructed left-deep tree.

