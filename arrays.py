import re

item = 'MainNode_ParentNode_1_SubNode_0_SubSubNode_1_SubSubSubNode_1_AtrNode'
node_metadata = item.split('_')
node_key = ""
data_key = ""

for i in range(1, len(node_metadata) - 3):
    node_key += ('_' if i!=1 else '') + node_metadata[i]
    
#print(node_metadata[-5])

print(node_key)


if "MainNode_Parent" == item:
    print(True)
else:
    print(False)

my_list = ['abc-123', 'def-456', 'ghi-789', 'abc-456', 'abc','def', "abc", "12312321abc"]
matchers = ['abc','def']
#matching = [s for s in my_list if any(xs in s for xs in matchers)]
#matching = [s for s in my_list if "abc" == s]
print(sum(any("abc" in L for m in my_list) for L in my_list))

print(re.sub('-[0-9]+$', '', "abc-123-abc-1"))