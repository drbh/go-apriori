package main

import (
	"fmt"
	"strings"

	"io/ioutil"

	"github.com/kniren/gota/dataframe"
	// "github.com/kniren/gota/series"

	"github.com/deckarep/golang-set"
	"sort"
	// "strings"
)

func main() {

	b, err := ioutil.ReadFile("BreadBasket_DMS.csv") // just pass the file name
	if err != nil {
		fmt.Print(err)
	}

	str := string(b) // convert

	csvStr := str

	df := dataframe.ReadCSV(strings.NewReader(csvStr))
	fmt.Println(df)
	nr, _ := df.Dims()
	fmt.Println(nr)

	nr = 100
	fmt.Println("Making List for Algo")
	var mybiglist [][]string
	for i := 1; i < nr; i++ {
		fd := df.Filter(dataframe.F{"Transaction", "==", i})
		col := fd.Select([]string{"Item"})
		txItems := col.Records()[1:]

		// fmt.Println("-")

		var mylist []string

		for i := 0; i < len(txItems); i++ {
			item := txItems[i][0]
			mylist = append(mylist, item)
			// f
		}
		// fmt.Println(mylist)
		mybiglist = append(mybiglist, mylist)

		if i%5000 == 0 {
			fmt.Println(i)
		}

	}

	dataset := mybiglist
	// fmt.Println(dataset)
	fmt.Println("Run matching algo")

	minimumSupport := 0.1 //2 //07
	// minConfidence := 0.1  //1  //05

	apriori(dataset, minimumSupport) //, minConfidence)

}

func apriori(dataset [][]string, support float64) { //, confidence float64) {

	elements := elements(dataset)
	freqSet := make(map[string]float64)
	largeSet := make(map[int]mapset.Set)
	oneCSet := returnItemsWithMinSupport(elements, dataset, support, &freqSet)

	currentLSet := oneCSet

	// fmt.Println("\nelements")
	// fmt.Println(elements)
	// fmt.Println("\nfreqSet")
	// fmt.Println(freqSet)
	// fmt.Println("\nlargeSet")
	// fmt.Println(largeSet)
	// fmt.Println("\noneCSet")
	// fmt.Println(oneCSet)
	// fmt.Println("\ncurrentLSet")
	// fmt.Println(currentLSet)
	// fmt.Println(currentLSet.Cardinality())

	k := 2

	for currentLSet.Cardinality() != 0 {
		largeSet[k-1] = currentLSet
		currentLSet = joinSet(currentLSet, k)
		currentCSet := returnItemsWithMinSupport(currentLSet, dataset, support, &freqSet)
		currentLSet = currentCSet
		k = k + 1
	}

	// fmt.Println(largeSet)

	items := largeSet[2]

	// for i := 0; i < items; i++ {
	fmt.Println(items)
	// 	fmt.Println("")
	// }

}

func returnItemsWithMinSupport(itemSet mapset.Set, dataset [][]string, minSupport float64, freqSet *map[string]float64) mapset.Set {

	localItemSet := mapset.NewSet()
	localSet := make(map[string]float64)

	for _, item := range itemSet.ToSlice() {
		dkey := strings.Split(item.(string), "-")
		sort.Strings(dkey)
		for _, line := range dataset {
			if contains(line, dkey) {
				key := strings.Join(dkey, "-")
				(*freqSet)[key] += 1.0
				localSet[key] += 1.0
			}
		}
	}

	for item, count := range localSet {
		support := count / float64(len(dataset))

		if support >= minSupport {

			fmt.Println(item)
			fmt.Println(support)

			localItemSet.Add(item)
		}
	}

	return localItemSet

}

func joinSet(itemSet mapset.Set, length int) mapset.Set {

	ret := mapset.NewSet()

	for _, i := range itemSet.ToSlice() {
		for _, j := range itemSet.ToSlice() {
			i := i.(string)
			j := j.(string)

			i_a := strings.Split(i, "-")
			j_a := strings.Split(j, "-")

			dkey := (union(i_a, j_a))
			if len(dkey) == length {
				sort.Strings(dkey)
				key := strings.Join(dkey, "-")
				ret.Add(key)

			}
		}
	}
	return ret
}

func union(a []string, b []string) []string {

	ret := mapset.NewSet()

	for _, v := range a {
		ret.Add(v)
	}
	for _, v := range b {
		ret.Add(v)
	}
	rets := []string{}
	for _, v := range ret.ToSlice() {
		rets = append(rets, v.(string))
	}
	return rets
}

func elements(dataset [][]string) mapset.Set {

	ret := mapset.NewSet()

	for i := 0; i < len(dataset); i++ {
		for j := 0; j < len(dataset[i]); j++ {
			if ret.Contains(dataset[i][j]) == false {
				ret.Add(dataset[i][j])
			}
		}
	}
	return ret
}

func contains_dataset(s [][]string, e []string) bool {
	ret := false
	for _, v := range s {
		ret = contains(v, e)
		if ret == true {
			break
		}
	}
	return ret
}

func contains_element(s []string, e string) bool {
	ret := false
	for _, a := range s {
		if a == e {
			ret = true
			break
		}
	}
	return ret
}

func contains(s []string, e []string) bool {
	count := 0
	if len(s) < len(e) {
		return false
	}
	mm := make(map[string]bool)
	for _, a := range e {
		mm[a] = true
	}

	for _, a := range s {
		if _, ok := mm[a]; ok {
			count += 1
		}
	}
	return count == len(e)
}
