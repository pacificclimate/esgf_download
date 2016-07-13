library(ncdf4.helpers)
library(RSQLite)

split.fn.cmip3 <- function(filenames) {
  split.fn <- as.data.frame(do.call(rbind, strsplit(filenames, "/")), stringsAsFactors=FALSE)
  lengths <- sapply(strsplit(filenames, "/"), length)
  split.fn <- split.fn[,(ncol(split.fn) - 6):ncol(split.fn)]
  if(length(unique(lengths)) != 1)
    browser()
  
  colnames(split.fn) <- c("sres.expt", "field.type", "time.res", "variable", "model", "run", "file")
  return(split.fn)
}

get.split.filename <- function(cmip5.file) {
  split.path <- strsplit(cmip5.file, "/")[[1]]
  fn.split <- strsplit(tail(split.path, n=1), "_")[[1]]
  names(fn.split) <- c("var", "tres", "model", "emissions", "run", "trange", rep(NA, max(0, length(fn.split) - 6)))
  fn.split[length(fn.split)] <- strsplit(fn.split[length(fn.split)], "\\.")[[1]][1]
  fn.split[c('tstart', 'tend')] <- strsplit(fn.split['trange'], "-")[[1]]
  fn.split
}


get.time.range <- function(filenames) {
  f <- nc_open(unlist(filenames[1]))
  ts <- nc.get.time.series(f)
  nc_close(f)

  time.range <- t(sapply(filenames, function(fn) { f <- nc_open(fn); ts <- nc.get.time.series(f); nc_close(f); cat(paste(fn, "done\n")); return(c(as.character(range(ts)), as.character(length(ts)))) }))
  return(data.frame(time.start=as.POSIXct(time.range[,1]), time.end=as.POSIXct(time.range[,2]), time.length=as.numeric(time.range[,3]), stringsAsFactors=FALSE))
}

create.cmip5.symlink.tree <- function(res, symlink.root.dir) {
  res$version <- paste("v", res$version, sep="")
  grouping <- paste(res$emissions, res$var, res$model, res$run, res$tres, res$version, sep="_")
  file.to.use <- tapply(seq_along(grouping), grouping, function(x) {
    if(length(x) == 1) {
      return(x)
    } else {
      date.range <- range(as.numeric(c(res$tstart[x], res$tend[x])))
      files.including.full.range <- as.numeric(res$tstart[x]) <= date.range[1] & as.numeric(res$tend[x]) >= date.range[2]
      if(any(files.including.full.range)) {
        return(x[files.including.full.range][1])
      } else {
        return(NA)
      }
    }
  })

  sapply(file.to.use, function(f) {
    ## FIXME: Should handle cases where length(f) != 1 (choose latest version)
    stopifnot(length(f) == 1)
    if(is.na(f))
      return(FALSE)
    file.details <- res[f,]
    symlink.dir <- paste(c(symlink.root.dir, file.details[c("institute", "model", "emissions", "tres")], "atmos", file.details[c("tres", "run", "version", "var")]), collapse="/")
    if(!file.exists(symlink.dir))
      stopifnot(dir.create(symlink.dir, recursive=TRUE))
    print(paste(f, paste(symlink.dir, basename(as.character(file.details["fullname"])), sep="/"), sep=": "))
    if(!file.exists(paste(symlink.dir, basename(as.character(file.details["fullname"])), sep="/")))
      stopifnot(file.symlink(as.character(file.details["fullname"]), symlink.dir))
  })
}

in.between <- function(tstart, tend) {
  stopifnot(length(tstart) == length(tend))
  return(sapply(1:length(tstart), function(idx) { any((tstart[idx] > tstart & tstart[idx] < tend) | (tend[idx] > tstart & tend[idx] < tend)) } ))
}

get.data.aggs <- function(tstart, tend) {
  stopifnot(length(tstart) == length(tend))
  return(lapply(1:length(tstart), function(idx) {
    which((tstart >= tstart[idx] & tend <= tend[idx] & !(tend == tend[idx] & tstart == tstart[idx])))
  }))
}

aggregate.data <- function(res, dry.run=FALSE) {
  date.range <- range(as.numeric(c(res$tstart, res$tend)))

  ## FIXME: Build path from scratch here.
  max.version <- max(res$version)
  split.file <- strsplit(res$fullname[1], "/")[[1]]
  base.path <- paste(split.file[1:(which.max(split.file == res$institute[1]) - 1)], collapse="/")
  path <- paste(base.path, paste(c(res[1,c("institute", "model", "emissions", "tres")], "atmos", res$tres[1], res$run[1], paste("v", max.version, sep=""), res$var[1]), collapse="/"), sep="/")
  new.file <- paste(path, paste(paste(res$var[1], res$tres[1], res$model[1], res$emissions[1], res$run[1], paste(sprintf("%08i", date.range), collapse="-"), sep="_"), "nc", sep="."), sep="/")
  cmd <- paste("ncrcat", paste(shQuote(res$fullname), collapse=" "), shQuote(new.file), sep=" ")
  print(cmd)
  if(!dry.run)
    stopifnot(system(cmd) == 0)
}

get.agg.subset <- function(sub.dat, sub.idx, cur.version, dry.run, arm.deletion.code, res) {
  data.aggs <- get.data.aggs(sub.dat$tstart, sub.dat$tend)
  data.aggs.numfiles <- sapply(data.aggs, length)
  sub.dat.noagg <- sub.dat[data.aggs.numfiles < 2,]
  sub.idx.noagg <- sub.idx[data.aggs.numfiles < 2]

  if(any(in.between(sub.dat.noagg$tstart, sub.dat.noagg$tend) & sub.dat.noagg$version != cur.version)) {
    ## There's screwy data in which one version uses different time slicing than the other. Only use this version, and only pass on this version.
    cur.vers.sub <- sub.dat$version == cur.version
    return(get.agg.subset(sub.dat[cur.vers.sub,], sub.idx[cur.vers.sub], cur.version, dry.run, arm.deletion.code, res))
  }

  ret <- list(noagg.idx=sub.idx.noagg)
  
  overall.range <- as.numeric(range(c(sub.dat.noagg$tstart, sub.dat.noagg$tend)))
  full.range.subsets <- apply(data.frame(as.numeric(sub.dat$tstart), as.numeric(sub.dat$tend)), 1, function(trange) { all(trange == overall.range) })
  
  ## Need to identify any complete subsets FROM THIS VERSION; need to remove any incomplete subsets (and flag for deletion?)
  cur.vers.subsets <- sub.dat$version[full.range.subsets] == cur.version
  if(any(cur.vers.subsets)) {
    ## We found a subset for the current version covering the whole range. Whoopee! Use it.
    ret$idx <- sub.idx[which(full.range.subsets)[cur.vers.subsets]]
  } else {
    ## There isn't a subset, but there also isn't any conflicting data. Choose newest.
    if(any(in.between(sub.dat.noagg$tstart, sub.dat.noagg$tend))) {
      ## Error condition
      ret$idx <- c()
      browser()
    } else {
      range.factor <- factor(paste(sub.dat.noagg$tstart, sub.dat.noagg$tend, sep="-"))
      ret$idx <- tapply(sub.idx.noagg, range.factor, function(r.idx) {
        return(r.idx[which.max(res$version[r.idx])])
      })
    }
  }
  ## Delete current version subsets, since we aren't using them and they're likely old / broken.
  if(arm.deletion.code && !dry.run) {
    cur.vers.aggs <- data.aggs.numfiles >= 2 & sub.dat$version == cur.version & !full.range.subsets
    if(any(cur.vers.aggs)) {
      print(paste(c("Deleting files", sub.dat$fullname[cur.vers.aggs]), collapse=" "))
      lapply(sub.dat$fullname[cur.vers.args], unlink)
    } else {
      print(paste(c("Would have deleted files", sub.dat$fullname[cur.vers.aggs]), collapse=" "))
    }
  }
  return(ret)
}

aggregate.cmip5 <- function(res, dry.run=FALSE, exclude.files=c(), arm.deletion.code=FALSE) {
  res$tstart <- as.numeric(res$tstart)
  res$tend <- as.numeric(res$tend)
  grouping <- paste(res$emissions, res$var, res$model, res$run, res$tres, sep="_")
  tapply(seq_along(grouping), grouping, function(x) {
    #exclude.mask <- normalizePath(res$fullfile[x]) %in% exclude.files
    #x <- x[!exclude.mask]
    if(length(x) == 1) {
      return(x)
    } else {
      ## Search for a record containing all of the data in the other files
      ## Output from each iteration: Aggregation or file list for current run, non-aggregate file list as input to next run.
      vers.factor <- factor(res$version[x])
      vers.sorted <- as.character(sort(as.numeric(levels(vers.factor))))
      vers.subsets <- tapply(x, vers.factor, c, simplify=FALSE)
      prev.vers.noagg <- c()
      
      ## Accumulate stacked data, culling old version data as needed, and only keeping one version if there are conflicts.
      for(i in 1:length(vers.sorted)) {
        vers.idx <- vers.sorted[i]
        sub.idx <- c(vers.subsets[[vers.idx]], prev.vers.noagg)
        sub.dat <- res[sub.idx,]

        ## Just in case...
        if(length(vers.subsets[[vers.idx]]) == 0)
          next

        new.dat <- get.agg.subset(sub.dat, sub.idx, vers.sorted[i], dry.run, arm.deletion.code, res)
        vers.subsets[[vers.idx]] <- new.dat$idx
        prev.vers.noagg <- new.dat$noagg.idx
      }

      ## For the identified subsets, aggregate if necessary.
      lapply(vers.subsets, function(ss) {
        if(length(ss) > 1)
          aggregate.data(res[ss,], dry.run)
      })
    }
  })
  invisible(0)
}

aggregate.and.rename.cmip3.to.cmip5 <- function(res, base.path, symlink.dir, file.dir="cmip3", dry.run=TRUE) {
  grouping <- paste(res$sres.expt, res$variable, res$model, res$run, sep="_")
  file.to.use <- tapply(seq_along(grouping), grouping, function(x) {
    if(length(x) == 1) {
      return(x)
    } else {
      ## Search for a record containing all of the data in the other files
      date.range <- range(c(res$time.start[x], res$time.end[x]))
      files.including.full.range <- res$time.start[x] == date.range[1] & res$time.end[x] == date.range[2]
      num.days <- sum(res$time.length[x[!files.including.full.range]])
      files.subset.other.files <- sapply(seq_along(x), function(f.id) {
        tstarts <- as.numeric(res$time.start[x])
        tends <- as.numeric(res$time.end[x])
        exact.matches <- tstarts[f.id] == tstarts & tends[f.id] == tends
        subset.bool <- tstarts[f.id] >= tstarts[-f.id] & tends[f.id] <= tends[-f.id]
        omit.data <- as.logical((which(which(exact.matches) == f.id) + 1) %% 2)
        return(omit.data | any(subset.bool & !exact.matches[-f.id]))
      })
      full.range.files.filtered <- files.including.full.range & res$time.length[x] == num.days
      if(any(files.including.full.range)) {
        if(any(full.range.files.filtered)) {
          return(x[full.range.files.filtered][1])
        } else {
          return(NA)
        }
      } else {
        return(x[!files.subset.other.files])
      }
    }
  })

  ## Check for problems with data (overlapping data)
  overlapping.stuff <- sapply(file.to.use, function(x) {
    any(is.na(x)) || (length(x) > 1 && any(sapply(seq_along(x), function(idx) {
      any((res$time.start[x[idx]] >= res$time.start[x[-idx]] & res$time.start[x[idx]] <= res$time.end[x[-idx]]) | (res$time.end[x][idx] >= res$time.start[x[-idx]] & res$time.end[x[idx]] <= res$time.end[x[-idx]]))
    })))
  })

  if(any(is.na(overlapping.stuff)) || any(overlapping.stuff)) {
    print("Overlapping records detected... omitting.")
    print(lapply(file.to.use[overlapping.stuff], function(x) { res$fullname[x] }))
  }
  
  ## Create symlinks and cat files together
  lapply(file.to.use[!overlapping.stuff], function(x) {
    date.range <- new.file <- old.file <- NA
    script.cmd <- ""

    expt.name <- res$sres.expt[x[1]]
    if(expt.name == "20c3m")
      expt.name <- "historical"
    new.path <- paste(base.path, "..", symlink.dir, expt.name, res$variable[x[1]], res$model[x[1]], res$run[x[1]], sep="/")

    date.range <- range(c(res$time.start[x], res$time.end[x]))

    if(length(x) == 1) {
      old.file <- paste(base.path, res$sres.expt[x][1], res$field.type[x][1], res$time.res[x][1], res$variable[x][1], res$model[x][1], res$run[x][1], res$file[x][1], sep="/")
    } else {
      old.file <- paste(dirname(res$fullname[x][1]), "INTERIM.nc", sep="/")
      cmd <- paste("ncrcat", paste(res$fullname[x], collapse=" "), old.file, sep=" ")
      print(cmd)
      if(!dry.run)
        system(cmd)
    }

    old.file.linkto <- paste("../../../../../", file.dir, "/", substr(old.file, nchar(base.path) + 1, nchar(old.file)), sep="")

    if(file.exists(old.file) && !dry.run) {
      new.file <- paste(new.path, paste(paste(res$variable[x][1], "day", gsub("_", "-", res$model[x][1]), expt.name, res$run[x][1], paste(format(date.range, "%Y%m%d"), collapse="-"), sep="_"), "nc", sep="."), sep="/")

      ## Create new path, create symlink
      if(!file.exists(new.path))
        stopifnot(dir.create(new.path, recursive=TRUE))
      if(!file.exists(new.file))
        if(!file.symlink(old.file.linkto, new.file)) {
          file.remove(new.file)
          stopifnot(file.symlink(old.file.linkto, new.file))
        }
    }
  })
}

get.file.metadata.cmip3 <- function(path.to.files) {
  file.list <- list.files(path.to.files, full.names=TRUE, recursive=TRUE, pattern="\\.nc$")
  file.list <- gsub("//", "/", file.list)
  
  ## Need to remove land stuff...
  file.list <- file.list[grepl("/atm/", file.list)]

  file.list.info <- cbind(file.info(file.list), split.fn.cmip3(file.list), get.time.range(file.list), fullname=file.list, version="v20070101", stringsAsFactors=FALSE)
  file.list.info
}

get.file.metadata.cmip5 <- function(path.to.files, pattern="\\.nc$") {
  file.list <- list.files(path.to.files, full.names=TRUE, recursive=TRUE, pattern=pattern)
  file.list <- gsub("//", "/", file.list)
  isValidVar <- function(x) { # Filters out fx vars since they aren't handled correctly
     return(!grepl('fx', x))
  }
  file.list <- Filter(isValidVar, file.list)
  fullfile.split <- strsplit(file.list, "/")
  file.data <- t(sapply(file.list, get.split.filename))
  institutes <- sapply(1:length(fullfile.split), function(x) { fs <- fullfile.split[[x]]; inst <- fs[which(fs == file.data[x,"model"]) - 1]; return(inst) } )
  versions <- as.numeric(sapply(1:length(fullfile.split), function(x) { fs <- fullfile.split[[x]]; vers <- fs[which(fs == file.data[x,"model"]) + 6]; return(substr(vers, 2, nchar(vers))) } ))
  
  file.list.info <- cbind(file.info(file.list), file.data, fullname=file.list, version=versions, institute=institutes, stringsAsFactors=FALSE)
  file.list.info
}

get.exclusion.list <- function(path.to.transfer.db, base.path) {
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = path.to.transfer.db)

  query <- paste("SELECT local_image FROM transfert WHERE status='waiting';")
  result <- dbSendQuery(con, query)
  exclusion.list <- normalizePath(paste(base.path, fetch(result, -1), sep="/"))
  return(exclusion.list)
}
