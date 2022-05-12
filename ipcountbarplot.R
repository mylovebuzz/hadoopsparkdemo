args <- commandArgs()
#print(args[6])
for (arg in args) {
    print(arg)
}
ipcount <- read.csv(args[6])
barplot(ipcount$Count, names.arg = ipcount$IP, xlab = "address", ylab = "number", main = "access statistic by IP", border = "red", col = "blue", las = 2)
dev.off()
