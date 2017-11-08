# Purpose:
Audit_Star is a database building block that is intended to provide a standard, easy to use mechanism for capturing row-level data changes in your Postgres databases.  It is to provide support for those scenarios where we do not already have another auditing mechanism in place (such as historian functionality or special application level events).

# Benefits:
The general benefits of auditing are well known.  Aside from capturing a historical perspective of the data, it also provides a mechanism to assist in troubleshooting/debugging certain types of problems/issues (particularly those that can be tied to recent data changes).  A secondary benefit includes providing a mechanism to communicate to ETL routines or downstream systems that a change (specifically a "delete") has taken place.

Some of the benefits that are specific to using this particular approach include the following:

* There is no maintenance required
* There is no custom code required
* The auditing mechanism never breaks, even when columns are added or removed from a table
* It is easy to apply/deploy
* It can be configured/customized (if needed) via a simple config file

# How Does it Work
This auditing mechanism involves (automatically) generating a number of database components for each table that is to be audited:

* Audit Table - Each "source" table will have a corresponding audit table (to hold the RECENT history of changes)
* Audit Trigger - Each source table will have a DML Trigger that propagates the data changes to its audit table
* Audit View - Each table will have a View that aids in querying/presenting the (JSON) data contained in the audit table

The schema of the audit tables do NOT match/mirror the source table they are auditing.  Rather, the audit tables have a single column that holds a JSON representation of the entire "before" row, and a second column that holds a JSON representation of the diff between the "before" and "after" rows.  The audit trigger handles creating the JSON representation of the changes, and inserting it into the audit table.  The trigger also handle creating new audit table partitions as needed.  The audit view converts the JSON back to a row-based representation of the data changes, so that the changes are easier to review.  The view also stitches together certain data from the source table (as in the case of initial inserts).
