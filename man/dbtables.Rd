% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/source.R
\name{dbtables}
\alias{dbtables}
\title{Configure database tables connection}
\usage{
dbtables(connection, tables, schema = "public")
}
\arguments{
\item{connection}{Database connection.}

\item{tables}{Vector of tables names to use in the source.}

\item{schema}{Name of the schema to connect to.}
}
\value{
Object of class 'db'.
}
\description{
The object should be used as `dtconn` argument of \link{set_source}.
}
