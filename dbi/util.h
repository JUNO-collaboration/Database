#ifndef dbi_util_h
#define dbi_util_h

#include <tuple>
#include <type_traits>
#include <sstream>

namespace dbi {
    // helper to convert from string to other types
    template<typename T>
    void dbi_record_cnv(const std::string& from, T& to) {
        std::stringstream ss;
        ss << from;
        ss >> to;
    }

    template<>
    void dbi_record_cnv<std::string>(const std::string& from, std::string& to) {
        to = from;
    }

    // helper for ResultSet
    template<size_t Index, size_t IndexR, 
             typename TInput, typename... Args, std::enable_if_t<Index == 0>* = nullptr>
    void tuple_element_helper(const TInput& , std::tuple<Args...>& ) {
        // std::cout << "Testing "
        //           << " Index: " << Index
        //           << " IndexR: " << IndexR
        //           << std::endl;
    }

    template<size_t Index, size_t IndexR, 
             typename TInput, typename... Args, std::enable_if_t<Index != 0>* = nullptr>
        void tuple_element_helper(const TInput& ir, std::tuple<Args...>& tuple) {

        // std::cout << " Index: " << Index << " IndexR: " << IndexR << std::endl;
        auto& v1 = std::get<IndexR>(tuple);
        typedef typename std::decay<decltype(v1)>::type T;
        auto v2 = ir[IndexR];

        dbi_record_cnv(v2, v1);
        // std::cout << " tuple: " << std::get<IndexR>(tuple) << std::endl;

        tuple_element_helper<Index-1, IndexR+1>(ir, tuple);
    }

    // helper for is tuple
    template<typename T>
        struct is_tuple_impl : std::false_type {};

    template<typename... Ts>
        struct is_tuple_impl<std::tuple<Ts...>> : std::true_type {};

    template<typename T>
        struct is_tuple : is_tuple_impl<std::decay_t<T>> {};
}

#endif
