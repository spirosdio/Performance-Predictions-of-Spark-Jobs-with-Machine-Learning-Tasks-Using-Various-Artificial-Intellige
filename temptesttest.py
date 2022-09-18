array1 = [1, 99, 3, 4]


array2 = [5, 6, 7, 8]

# for everyelement in both arrays keep the max
quortersHeap = [max(x, y) for x, y in zip(array1, array2)]

print(quortersHeap)
