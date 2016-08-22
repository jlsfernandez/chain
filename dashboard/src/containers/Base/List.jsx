import { connect as reduxConnect } from 'react-redux'
import actions from '../../actions'
import ItemList from '../../components/ItemList'

export const mapStateToProps = (type, itemComponent) => (state) => ({
  pages: state[type].pages,
  currentPage: state[type].currentPage,
  query: state[type].currentQuery,
  type: type,
  listItemComponent: itemComponent
})

export const mapDispatchToProps = (type) => (dispatch) => ({
  getNextPage: () => dispatch(actions[type].displayNextPage()),
  getPrevPage: () => dispatch(actions[type].decrementPage()),
  showCreate: () => dispatch(actions[type].showCreate()),
  submitQuery: (query) => dispatch(actions[type].submitQuery(query))
})

export const connect = (state, dispatch) => reduxConnect(
  state,
  dispatch
)(ItemList)