package org.apache.hadoop.net;

class VirtualizedInnerNode extends InnerNode {

	public VirtualizedInnerNode(String name, String location, InnerNode parent,
			int level) {
		super(name, location, parent, level);
	}

	public VirtualizedInnerNode(String name, String location) {
		super(name, location);
	}

	public VirtualizedInnerNode(String path) {
		super(path);
	}

	@Override
	boolean isRack() {
		// it is node group
		if (getChildren().isEmpty()) {
			return false;
		}

		Node firstChild = children.get(0);

		if (firstChild instanceof InnerNode) {
			Node firstGrandChild = (((InnerNode) firstChild).children).get(0);
			if (firstGrandChild instanceof InnerNode) {
				// it is datacenter
				return false;
			} else {
				return true;
			}
		}
		return false;
	}

	/**
	 * Judge if this node represents a node group (hypervisor)
	 * 
	 * @return true if it has no child or its children are not InnerNodes
	 */
	boolean isNodeGroup() {
	  if (children.isEmpty()) {
		return true;
	  }

	  Node firstChild = children.get(0);
	  if (firstChild instanceof InnerNode) {
		// it is rack or datacenter
		return false;
	  }
	  return true;
	}

	@Override
	protected InnerNode doCreateParentNode(String parentName) {
		return new VirtualizedInnerNode(parentName, getPath(this), this,
				this.getLevel() + 1);
	}

	@Override
	protected boolean doAreChildrenLeaves() {
		return isNodeGroup();
	}

}
