# Declare micro-libs:
add_library(quickstep_utility_meta_Common ../../empty_src.cpp Common.hpp)
add_library(quickstep_utility_meta_Dispatchers ../../empty_src.cpp Dispatchers.hpp)
add_library(quickstep_utility_meta_Math ../../empty_src.cpp Math.hpp)
add_library(quickstep_utility_meta_TMP ../../empty_src.cpp TMP.hpp)
add_library(quickstep_utility_meta_TypeList ../../empty_src.cpp TypeList.hpp)
add_library(quickstep_utility_meta_TypeListMetaFunctions ../../empty_src.cpp TypeListMetaFunctions.hpp)

# Link dependencies:
target_link_libraries(quickstep_utility_meta_Dispatchers
                      glog
                      quickstep_utility_meta_Common
                      quickstep_utility_meta_TypeList)
target_link_libraries(quickstep_utility_meta_TMP
                      quickstep_utility_meta_Common
                      quickstep_utility_meta_Dispatchers
                      quickstep_utility_meta_TypeList)
target_link_libraries(quickstep_utility_meta_TypeList
                      quickstep_utility_meta_Common
                      quickstep_utility_meta_TypeListMetaFunctions)
target_link_libraries(quickstep_utility_meta_TypeListMetaFunctions
                      quickstep_utility_meta_Common)
